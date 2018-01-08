package com.bazaarvoice.emodb.stash.emr.databus;

import com.bazaarvoice.emodb.stash.emr.ContentEncoding;
import com.bazaarvoice.emodb.stash.emr.DocumentId;
import com.bazaarvoice.emodb.stash.emr.sql.DocumentSchema;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import javax.annotation.Nullable;
import javax.ws.rs.core.UriBuilder;
import java.io.Serializable;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

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
                .help("Databus API key");
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
        argParser.addArgument("--batchInterval")
                .setDefault("PT5M")
                .help("Streaming batch interval");

        Namespace ns = argParser.parseArgs(args);

        String cluster = ns.getString("cluster");
        String subscriptionName = ns.getString("subscriptionName");
        String subscriptionCondition = ns.getString("subscriptionCondition");
        String apiKey = ns.getString("apikey");
        String destination = ns.getString("destination");
        String master = ns.getString("master");
        Duration batchInterval = Durations.milliseconds(java.time.Duration.parse(ns.getString("batchInterval")).toMillis());

        String zkConnectionString = ns.getString("zkConnectionString");
        String zkNamespace = ns.getString("zkNamespace");
        String emoUrlString = ns.getString("emoUrl");

        URI emoUri = null;
        if (emoUrlString != null) {
            emoUri = UriBuilder.fromUri(emoUrlString)
                    .replacePath(null)
                    .build();
        }

        DatabusDiscovery.Builder databusDiscoveryBuilder = DatabusDiscovery.builder(cluster)
                .withSubscription(subscriptionName)
                .withZookeeperDiscovery(zkConnectionString, zkNamespace)
                .withDirectUri(emoUri);

        DatabusReceiver databusReceiver = new DatabusReceiver(databusDiscoveryBuilder, subscriptionName, subscriptionCondition, apiKey);
        
        new DatabusAccumulator().runAccumulator(databusReceiver, destination, master, batchInterval);
    }

    public void runAccumulator(final DatabusReceiver databusReceiver, final String destination,
                               @Nullable final String master, Duration batchInterval) throws InterruptedException {

        SparkSession sparkSession = SparkSession.builder()
                .appName("DatabusAccumulator")
                .master(master)
                .getOrCreate();

        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, batchInterval);
        Broadcast<String> broadcastDestination = streamingContext.sparkContext().broadcast(destination);

        JavaDStream<DatabusEvent> eventStream = streamingContext.receiverStream(databusReceiver);
        // Group events by document id
        JavaPairDStream<DocumentId, DatabusEvent> eventsById = eventStream.mapToPair(
                event -> new Tuple2<>(event.getDocumentMetadata().getDocumentId(), event));
        // Dedup events within the stream, keeping only the most recent if multiple updates occurred
        JavaPairDStream<DocumentId, DatabusEvent> dedupEvents =
                eventsById.reduceByKey(DatabusAccumulator::newestDocumentVersion);

        dedupEvents.foreachRDD((rdd, time) -> {
            SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());
            Dataset<Row> dataFrame = sqlContext.createDataFrame(
                    rdd.values().map(event -> toRow(event.getUpdateId(), event.getDocumentMetadata(),
                            ContentEncoding.LZ4, event.getJson(),
                            ZonedDateTime.ofInstant(Instant.ofEpochMilli(time.milliseconds()), ZoneOffset.UTC))),
                    DocumentSchema.SCHEMA);

            dataFrame.write().mode(SaveMode.Append).partitionBy(POLL_DATE, TABLE).parquet(broadcastDestination.value());
        });

        streamingContext.start();
        streamingContext.awaitTermination();
    }

    private static DatabusEvent newestDocumentVersion(DatabusEvent left, DatabusEvent right) {
        if (left.getDocumentMetadata().getDocumentVersion().compareTo( right.getDocumentMetadata().getDocumentVersion()) < 0) {
            return right;
        }
        return left;
    }
}
