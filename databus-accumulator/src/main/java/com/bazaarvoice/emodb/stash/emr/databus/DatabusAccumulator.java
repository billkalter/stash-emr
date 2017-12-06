package com.bazaarvoice.emodb.stash.emr.databus;

import com.bazaarvoice.emodb.stash.emr.DocumentId;
import com.bazaarvoice.emodb.stash.emr.DocumentMetadata;
import com.bazaarvoice.emodb.stash.emr.sql.DocumentSchema;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import javax.annotation.Nullable;
import javax.ws.rs.core.UriBuilder;
import java.io.Serializable;
import java.net.URI;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Future;

import static com.bazaarvoice.emodb.stash.emr.sql.DocumentSchema.POLL_DATE;
import static com.bazaarvoice.emodb.stash.emr.sql.DocumentSchema.TABLE;
import static com.bazaarvoice.emodb.stash.emr.sql.DocumentSchema.toRow;

public class DatabusAccumulator implements Serializable {

    public static void main(String args[]) throws Exception {
        ArgumentParser argParser = ArgumentParsers.newFor("DatabusAccumulator").addHelp(true).build();

        argParser.addArgument("--cluster")
                .required(true)
                .help("EmoDB cluster name");
        argParser.addArgument("--subscriptionName")
                .required(true)
                .help("Databus subscription name");
        argParser.addArgument("--subscriptionCondition")
                .required(true)
                .help("Databus subscription condition");
        argParser.addArgument("--apikey")
                .required(true)
                .help("Databus API key name");
        argParser.addArgument("--destination")
                .required(true)
                .help("Location where databus updates should be written");
        argParser.addArgument("--zkConnectionString")
                .help("ZooKeeper connection string (if using EmoDB host discovery)");
        argParser.addArgument("--zkNamespace")
                .help("ZooKeeper namespace (if using EmoDB host discovery)");
        argParser.addArgument("--emoUrl")
                .help("EmoDB URL (if using direct EmoDB access)");
        argParser.addArgument("--master")
                .help("Spark master URL");

        Namespace ns = argParser.parseArgs(args);

        List<Future> futures = Lists.newArrayListWithCapacity(1);
        futures.forEach(Futures::getUnchecked);

        String cluster = ns.getString("cluster");
        String subscriptionName = ns.getString("subscriptionName");
        String subscriptionCondition = ns.getString("subscriptionCondition");
        String apiKey = ns.getString("apikey");
        String destination = ns.getString("destination");
        DatabusReceiver databusReceiver;

        String zkConnectionString = ns.getString("zkConnectionString");
        String zkNamespace = ns.getString("zkNamespace");
        String emoUrl = ns.getString("emoUrl");

        if (zkConnectionString != null) {
            if (zkNamespace == null) {
                throw new ArgumentParserException("All ZooKeeper arguments are required for discovery", argParser);
            }
            if (emoUrl != null) {
                throw new ArgumentParserException("Only one Databus lookup method is permitted", argParser);
            }
            databusReceiver = DatabusReceiver.fromHostDiscovery(subscriptionName, subscriptionCondition, apiKey, cluster, zkConnectionString, zkNamespace);
        } else if (emoUrl == null) {
            throw new ArgumentParserException("One EmoDB discovery method is required", argParser);
        } else {
            if (zkNamespace != null) {
                throw new ArgumentParserException("Only one Databus lookup method is permitted", argParser);
            }
            URI uri = UriBuilder.fromUri(emoUrl)
                    .replacePath(null)
                    .build();
            databusReceiver = DatabusReceiver.fromHostAndPort(subscriptionName, subscriptionCondition, apiKey, cluster, uri);
        }

        String master = ns.getString("master");

        new DatabusAccumulator().runAccumulator(databusReceiver, destination, master);
    }

    public void runAccumulator(final DatabusReceiver databusReceiver, final String destination,
                               @Nullable final String master) throws InterruptedException {

        SparkConf sparkConf = new SparkConf().setAppName("DatabusAccumulator");
        if (master != null) {
            sparkConf.setMaster(master);
        }
        
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkConf, Durations.seconds(30));

        JavaReceiverInputDStream<Tuple2<DocumentMetadata, String>> eventStream = streamingContext.receiverStream(databusReceiver);
        // Group events by document id
        JavaPairDStream<DocumentId, Tuple2<DocumentMetadata, String>> eventsById = eventStream.mapToPair(
                tuple2 -> new Tuple2<>(tuple2._1.getDocumentId(), tuple2));
        // Dedup events within the stream, keeping only the most recent if multiple updates occurred
        JavaPairDStream<DocumentId, Tuple2<DocumentMetadata, String>> dedupEvents = eventsById.reduceByKey(this::newestDocumentVersion);

        dedupEvents.foreachRDD((rdd, time) -> {
            SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());
            Dataset<Row> dataFrame = sqlContext.createDataFrame(
                    rdd.values().map(tuple2 -> toRow(tuple2._1, tuple2._2, new Date(time.milliseconds()))),
                    DocumentSchema.SCHEMA);

            dataFrame.write().mode(SaveMode.Append).partitionBy(POLL_DATE, TABLE).parquet(destination);
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    private Tuple2<DocumentMetadata, String> newestDocumentVersion(Tuple2<DocumentMetadata, String> left, Tuple2<DocumentMetadata, String> right) {
        if (left._1.getDocumentVersion().compareTo( right._1.getDocumentVersion()) < 0) {
            return right;
        }
        return left;
    }
}
