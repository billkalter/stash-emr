package com.bazaarvoice.emodb.stash.emr.generator;

import com.bazaarvoice.emodb.stash.emr.DocumentId;
import com.bazaarvoice.emodb.stash.emr.DocumentMetadata;
import com.bazaarvoice.emodb.stash.emr.generator.io.StashIO;
import com.bazaarvoice.emodb.stash.emr.sql.DocumentSchema;
import com.fasterxml.jackson.databind.util.ISO8601Utils;
import com.google.common.base.Joiner;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.Seq;
import scala.collection.Seq$;
import scala.collection.mutable.Builder;

import javax.annotation.Nullable;
import javax.ws.rs.core.UriBuilder;
import java.net.URI;
import java.text.ParsePosition;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.List;
import java.util.Map;

import static com.bazaarvoice.emodb.stash.emr.generator.TableStatus.NA;
import static com.bazaarvoice.emodb.stash.emr.json.JsonUtil.parseJson;
import static com.bazaarvoice.emodb.stash.emr.sql.DocumentSchema.getJson;
import static com.bazaarvoice.emodb.stash.emr.sql.DocumentSchema.getMetadata;
import static com.bazaarvoice.emodb.stash.emr.sql.DocumentSchema.toPollTime;
import static com.google.common.base.Preconditions.checkArgument;

public class StashGenerator {

    private static final Logger _log = LoggerFactory.getLogger(StashGenerator.class);

    private static final DateTimeFormatter STASH_DIR_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss").withZone(ZoneOffset.UTC);

    public static void main(String args[]) throws Exception {
        ArgumentParser argParser = ArgumentParsers.newFor("StashGenerator").addHelp(true).build();

        argParser.addArgument("--cluster")
                .required(true)
                .help("EmoDB cluster name");
        argParser.addArgument("--apikey")
                .required(true)
                .help("EmoDB API key name");
        argParser.addArgument("--databusSource")
                .required(true)
                .help("Location where databus updates were written");
        argParser.addArgument("--stashRoot")
                .required(true)
                .help("Root Stash directory (ex: s3://emodb-us-east-1/stash/ci)");
        argParser.addArgument("--stashDate")
                .required(true)
                .help("Date for the Stash being generated");
        argParser.addArgument("--zkConnectionString")
                .help("ZooKeeper connection string (if using EmoDB host discovery)");
        argParser.addArgument("--zkNamespace")
                .help("ZooKeeper namespace (if using EmoDB host discovery)");
        argParser.addArgument("--emoUrl")
                .help("EmoDB URL (if using direct EmoDB access)");
        argParser.addArgument("--master")
                .help("Spark master URL");
        argParser.addArgument("--region")
                .help("Region where S3 bucket is located if 'stashRoot' is in S3. (default is EC2 host's region)");
        argParser.addArgument("--existingTablesFile")
                .help("Location of a file which contains a list of existing Emo tables, one per line. " +
                      "Useful for local testing without an EmoDB service to query for the list.");

        Namespace ns = argParser.parseArgs(args);

        String cluster = ns.getString("cluster");
        String apiKey = ns.getString("apikey");
        String databusSource = ns.getString("databusSource");
        URI stashRoot = URI.create(ns.getString("stashRoot"));
        ZonedDateTime stashDate = Instant
                .ofEpochMilli(ISO8601Utils.parse(ns.getString("stashDate"), new ParsePosition(0)).getTime())
                .atZone(ZoneOffset.UTC);
        String master = ns.getString("master");
        String region = ns.getString("region");
        String existingTablesFile = ns.getString("existingTablesFile");

        String zkConnectionString = ns.getString("zkConnectionString");
        String zkNamespace = ns.getString("zkNamespace");
        String emoUrlString = ns.getString("emoUrl");

        URI emoUri = null;
        if (emoUrlString != null) {
            emoUri = UriBuilder.fromUri(emoUrlString)
                    .replacePath(null)
                    .build();
        }

        DataStoreDiscovery.Builder dataStoreDiscoveryBuilder = DataStoreDiscovery.builder(cluster)
                .withZookeeperDiscovery(zkConnectionString, zkNamespace)
                .withDirectUri(emoUri);

        DataStore dataStore = new DataStore(dataStoreDiscoveryBuilder, apiKey);

        new StashGenerator().runStashGenerator(dataStore, databusSource, stashRoot, stashDate, master,
                Optional.fromNullable(region),
                Optional.fromNullable(existingTablesFile));
    }

    public void runStashGenerator(final DataStore dataStore, final String databusSource, final URI stashRoot,
                                  final ZonedDateTime stashTime, @Nullable final String master,
                                  Optional<String> region,
                                  Optional<String> existingTablesFile) throws Exception {

        SparkConf sparkConf = new SparkConf().setAppName("StashGenerator");
        if (master != null) {
            sparkConf.setMaster(master);
        }

        JavaSparkContext context = new JavaSparkContext(sparkConf);
        StashIO stashIO = StashIO.forStashAt(stashRoot, region);

        Tuple2<ZonedDateTime, String> priorStash = getPriorStash(stashIO);
        ZonedDateTime priorStashTime = priorStash._1;
        String priorStashDir = priorStash._2;
        String newStashDir = STASH_DIR_FORMAT.format(stashTime);

        checkArgument(priorStashTime.isBefore(stashTime), "Cannot create Stash older than existing latest Stash");
        _log.info("Creating Stash to {} merging with previous stash at {}", newStashDir, priorStashDir);
        
        // Get all tables that exist in Stash
        JavaPairRDD<String, Short> emoTables = getEmoTableNamesRDD(context, dataStore, existingTablesFile)
                .mapToPair(tableName -> new Tuple2<>(tableName, TableStatus.EXISTS_IN_EMO));

        // Get all tables that have been updated since the last stash
        JavaPairRDD<String, Short> updatedTables = getDatabusEventsTablesRDD(context, databusSource, priorStashTime, stashTime)
                .mapToPair(tableName -> new Tuple2<>(tableName, TableStatus.CONTAINS_UPDATES));

        // Map all tables that exist in Emo with any that may have received updates since the last Stash
        JavaPairRDD<String, Short> allTables = emoTables.fullOuterJoin(updatedTables)
                .reduceByKey((left, right) -> new Tuple2<>(
                        Optional.of((short) (left._1.or(NA) | right._1.or(NA) | left._2.or(NA) | right._2.or(NA))), Optional.absent()))
                .mapValues(t -> (short) (t._1.or(NA) | t._2.or(NA)))
                .persist(StorageLevel.MEMORY_AND_DISK_SER_2());

        // For those tables which exist and have no updates they can be copied as-is
        JavaRDD<String> unmodifiedTables = allTables
                .filter(t -> TableStatus.existsInEmo(t._2) && !TableStatus.containsUpdates(t._2))
                .map(t -> t._1);

        JavaRDD<String> mergeTables = allTables
                .filter(t -> TableStatus.existsInEmo(t._2) && TableStatus.containsUpdates(t._2))
                .map(t -> t._1);

        JavaRDD<String> droppedTables = allTables
                .filter(t-> !TableStatus.existsInEmo(t._2))
                .map(t -> t._1)
                .sortBy(t -> t, true, 8);

        droppedTables.foreachPartitionAsync(tables -> {
            _log.info("The following tables contained updates since the last Stash but no longer exist in Emo: [{}]",
                    Joiner.on(",").join(tables));
        });

        List<JavaFutureAction<Void>> futures = Lists.newArrayListWithCapacity(2);

        futures.add(copyExistingStashTables(unmodifiedTables, stashIO, priorStashDir, newStashDir));

        futures.add(copyAndMergeUpdatedStashTables(context, mergeTables, databusSource, stashIO, priorStashDir, newStashDir, priorStashTime, stashTime));

        for (JavaFutureAction<Void> future : futures) {
            future.get();
        }
    }

    private JavaFutureAction<Void> copyExistingStashTables(final JavaRDD<String> tables,
                                         final StashIO stashIO, final String priorStashDir, final String newStashDir) {
        JavaPairRDD<String, String> tableFiles = tables.flatMapToPair(table ->
                stashIO.getTableFilesFromStash(priorStashDir, table)
                        .stream()
                        .map(file -> new Tuple2<>(table, file))
                        .iterator());

        return tableFiles.foreachAsync(t -> stashIO.copyTableFile(priorStashDir, newStashDir, t._1, t._2));
    }

    private JavaFutureAction<Void> copyAndMergeUpdatedStashTables(final JavaSparkContext context, final JavaRDD<String> tables,
                                                                  final String databusSource,
                                                                  final StashIO stashIO, final String priorStashDir, final String newStashDir,
                                                                  final ZonedDateTime priorStashTime, final ZonedDateTime stashTime) {

        // Get all documents from the updated tables

        JavaPairRDD<String, String> existingTableFiles = tables.flatMapToPair(table ->
                stashIO.getTableFilesFromStash(priorStashDir, table)
                        .stream()
                        .map(file -> new Tuple2<>(table, file))
                        .iterator());

        JavaPairRDD<DocumentMetadata, String> priorStashDocs = existingTableFiles
                .flatMap(t -> stashIO.readStashTableFile(priorStashDir, t._1, t._2))
                .mapToPair(json -> new Tuple2<>(parseJson(json, DocumentMetadata.class), json));


        JavaPairRDD<DocumentMetadata, String> updatedStashDocs =
                getDatabusEventsRDD(context, databusSource, priorStashTime, stashTime);

        JavaPairRDD<DocumentMetadata, String> allStashDocs =
                priorStashDocs
                        .union(updatedStashDocs)
                        .persist(StorageLevel.MEMORY_AND_DISK_SER_2());

        // Extract the document versions and select the latest

        JavaPairRDD<DocumentMetadata, Boolean> latestDocs = allStashDocs.keys()
                .keyBy(DocumentMetadata::getDocumentId)
                .reduceByKey((left, right) ->
                    left.getDocumentVersion().compareTo(right.getDocumentVersion()) > 0 ? left : right)
                .values()
                .filter(md -> !md.isDeleted())
                .mapToPair(md -> new Tuple2<>(md, true))
                .persist(StorageLevel.MEMORY_AND_DISK_SER_2());

        Map<String, Long> docCountsbyTable = latestDocs
                .mapToPair(md -> new Tuple2<>(md._1.getDocumentId().getTable(), true))
                .countByKey();

        // Re-join with the documents to get the most current documents by table
        JavaPairRDD<DocumentId, String> finalDocsByTable =
                latestDocs.join(allStashDocs)
                        .mapToPair(t -> new Tuple2<>(t._1.getDocumentId(), t._2._2))
                        .partitionBy(new DocumentPartitioner(docCountsbyTable));

        return finalDocsByTable.foreachPartitionAsync(t -> {
            _log.info("BJK: Partition contains: {}", Joiner.on(",").join(Iterators.transform(t, Tuple2::_1)));
        });
    }

    private Tuple2<ZonedDateTime, String> getPriorStash(StashIO stashIO) throws Exception {
       String latestFileDir = stashIO.getLatest();
        _log.info("Latest Stash found prior to the one being generated is {}", latestFileDir);
        ZonedDateTime priorStashDate = STASH_DIR_FORMAT.parse(latestFileDir, ZonedDateTime::from);
        return new Tuple2<>(priorStashDate, latestFileDir);
    }

    private JavaRDD<String> getEmoTableNamesRDD(JavaSparkContext context, DataStore dataStore, Optional<String> existingTablesFile) {
        if (existingTablesFile.isPresent()) {
            // A files was explicitly provided with the full list of EmoDB tables.
            _log.info("Emo table tables have been explicitly provided. This is likely undesirable in a non-test environment.");
            return context.textFile(existingTablesFile.get());
        }

        final List<JavaRDD<String>> allTableNameRDDs = Lists.newArrayList();
        Iterators.partition(dataStore.getTableNames(), 1000)
                .forEachRemaining(tableNames -> allTableNameRDDs.add(context.parallelize(tableNames)));

        switch (allTableNameRDDs.size()) {
            case 0:
                return context.emptyRDD();
            case 1:
                return allTableNameRDDs.get(0);
            default:
                return context.union(allTableNameRDDs.get(0), allTableNameRDDs.subList(1, allTableNameRDDs.size()));
        }
    }

    private JavaRDD<String> getDatabusEventsTablesRDD(JavaSparkContext context, String databusSource,
                                                      ZonedDateTime priorStashTime, ZonedDateTime stashTime) {

        final Seq<Object> pollDates = getPollDates(priorStashTime, stashTime);
        final SQLContext sqlContext = SQLContext.getOrCreate(context.sc());
        final Dataset<Row> dataFrame = sqlContext.read().schema(DocumentSchema.SCHEMA).parquet(databusSource);

        return dataFrame.select(dataFrame.col(DocumentSchema.TABLE))
                .where(dataFrame.col(DocumentSchema.POLL_DATE).isin(pollDates))
                .distinct()
                .map(value -> value.getString(0), Encoders.STRING())
                .toJavaRDD();
    }

    private JavaPairRDD<DocumentMetadata, String> getDatabusEventsRDD(JavaSparkContext context, String databusSource,
                                                                      ZonedDateTime priorStashTime, ZonedDateTime stashTime) {
        final Seq<Object> pollDates = getPollDates(priorStashTime, stashTime);
        final SQLContext sqlContext = SQLContext.getOrCreate(context.sc());
        final Dataset<Row> dataFrame = sqlContext.read().schema(DocumentSchema.SCHEMA).parquet(databusSource);

        return dataFrame
                .select(
                        dataFrame.col(DocumentSchema.TABLE), dataFrame.col(DocumentSchema.KEY),
                        dataFrame.col(DocumentSchema.VERSION), dataFrame.col(DocumentSchema.LAST_UPDATE_TS),
                        dataFrame.col(DocumentSchema.DELETED), dataFrame.col(DocumentSchema.JSON))
                .where(dataFrame.col(DocumentSchema.POLL_DATE).isin(pollDates))
                .toJavaRDD()
                .mapToPair(row -> new Tuple2<>(getMetadata(row), getJson(row)));
    }

    private Seq<Object> getPollDates(ZonedDateTime priorStashTime, ZonedDateTime stashTime) {
        Builder<Object, Seq<Object>> pollDates = Seq$.MODULE$.newBuilder();

        // Poll times work on day boundaries, so move to the start of day
        ZonedDateTime pollDate = priorStashTime.truncatedTo(ChronoUnit.DAYS);
        while (pollDate.isBefore(stashTime)) {
            pollDates.$plus$eq(toPollTime(pollDate));
            pollDate = pollDate.plusDays(1);
        }

        return pollDates.result();
    }
}
