package com.bazaarvoice.emodb.stash.emr.databus;

import com.bazaarvoice.emodb.stash.emr.ContentEncoding;
import com.bazaarvoice.emodb.stash.emr.sql.DocumentSchema;
import com.google.common.collect.Lists;
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
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import javax.annotation.Nullable;
import javax.ws.rs.core.UriBuilder;
import java.io.Serializable;
import java.net.URI;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.List;

import static com.bazaarvoice.emodb.stash.emr.sql.DocumentSchema.POLL_DATE;
import static com.bazaarvoice.emodb.stash.emr.sql.DocumentSchema.TABLE;
import static com.bazaarvoice.emodb.stash.emr.sql.DocumentSchema.toRow;

/**
 * Spark streaming job for listening to an EmoDB databus subscription and writing the events to a Parquet table.
 */
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
                .required(true)
                .help("Spark master URL");
        argParser.addArgument("--jsonEncoding")
                .choices("TEXT", "LZ4")
                .setDefault("LZ4")
                .help("Persisted encoding for JSON");
        argParser.addArgument("--receiverStreams")
                .type(Integer.class)
                .setDefault(2)
                .help("Number of receiver streams (default 2)");
        argParser.addArgument("--batchInterval")
                .setDefault("PT5M")
                .help("Streaming batch interval");
        argParser.addArgument("--batchPartitions")
                .type(Integer.class)
                .setDefault(8)
                .help("Number of partitions per batch");


        Namespace ns = argParser.parseArgs(args);

        String cluster = ns.getString("cluster");
        String subscriptionName = ns.getString("subscriptionName");
        String subscriptionCondition = ns.getString("subscriptionCondition");
        String apiKey = ns.getString("apikey");
        String destination = ns.getString("destination");
        String master = ns.getString("master");
        int receiverStreams = ns.getInt("receiverStreams");
        Duration batchInterval = Durations.milliseconds(java.time.Duration.parse(ns.getString("batchInterval")).toMillis());
        int batchPartitions = ns.getInt("batchPartitions");
        ContentEncoding contentEncoding = ContentEncoding.valueOf(ns.getString("jsonEncoding"));
        
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
        
        new DatabusAccumulator().runAccumulator(
                databusReceiver, destination, master, receiverStreams, batchInterval, batchPartitions, contentEncoding);
    }

    public void runAccumulator(final DatabusReceiver databusReceiver, final String destination,
                               @Nullable final String master, final int receiverStreams,
                               final Duration batchInterval, final int batchPartitions,
                               final ContentEncoding contentEncoding) throws InterruptedException {

        SparkSession sparkSession = SparkSession.builder()
                .appName("DatabusAccumulator")
                .master(master)
                .config("spark.streaming.stopGracefullyOnShutdown", true)
                .getOrCreate();

        JavaSparkContext sparkContext = new JavaSparkContext(sparkSession.sparkContext());
        JavaStreamingContext streamingContext = new JavaStreamingContext(sparkContext, batchInterval);
        Broadcast<String> broadcastDestination = streamingContext.sparkContext().broadcast(destination);

        JavaDStream<DatabusEvent> eventStream = streamingContext.receiverStream(databusReceiver);
        if (receiverStreams > 1) {
            List<JavaDStream<DatabusEvent>> eventStreams = Lists.newArrayListWithCapacity(receiverStreams - 1);
            for (int i=1; i < receiverStreams; i++) {
                eventStreams.add(streamingContext.receiverStream(databusReceiver));
            }
            eventStream = streamingContext.union(eventStream, eventStreams);
        }

        eventStream
                // Group events by document id
                .mapToPair(event -> new Tuple2<>(event.getDocumentMetadata().getDocumentId(), event))
                // Dedup events within the stream, keeping only the most recent if multiple updates occurred
                .reduceByKey(DatabusAccumulator::newestDocumentVersion)
                // Sort by table and repartition
                .transformToPair(rdd -> rdd.sortByKey(true, batchPartitions))
                // Write to parquet
                .foreachRDD((rdd, time) -> {
                        SQLContext sqlContext = SQLContext.getOrCreate(rdd.context());
                        Dataset<Row> dataFrame = sqlContext.createDataFrame(
                                rdd.values().map(event -> toRow(event.getUpdateId(), event.getDocumentMetadata(),
                                        contentEncoding, event.getJson(),
                                        ZonedDateTime.ofInstant(Instant.ofEpochMilli(time.milliseconds()), ZoneOffset.UTC))),
                                DocumentSchema.SCHEMA);

                        dataFrame.write().mode(SaveMode.Append).partitionBy(POLL_DATE, TABLE).parquet(broadcastDestination.value());
                });

        streamingContext.start();
        // TODO:  Introduce a means to gracefully shutdown then <code>call streamingContext.stop(true, true)</code>;
        //        The following works but Stash records stored in the pipeline and not yet persisted would be lost.
        //        This can be manually overcome by performing a databus replay, but it would be beneficial if
        //        graceful shutdown were at least programmatically possible.
        streamingContext.awaitTermination();
    }

    /**
     * Simple comparator which takes two databus events for the same document and returns the event with the most recent version.
     */
    private static DatabusEvent newestDocumentVersion(DatabusEvent left, DatabusEvent right) {
        if (left.getDocumentMetadata().getDocumentVersion().compareTo(right.getDocumentMetadata().getDocumentVersion()) < 0) {
            return right;
        }
        return left;
    }
}
