package com.bazaarvoice.emodb.stash.emr.generator;

import com.bazaarvoice.emodb.stash.emr.DocumentMetadata;
import com.bazaarvoice.emodb.stash.emr.generator.io.CloseableIterator;
import com.bazaarvoice.emodb.stash.emr.generator.io.StashFileWriter;
import com.bazaarvoice.emodb.stash.emr.generator.io.StashIO;
import com.bazaarvoice.emodb.stash.emr.generator.io.StashReader;
import com.bazaarvoice.emodb.stash.emr.generator.io.StashWriter;
import com.bazaarvoice.emodb.stash.emr.json.JsonUtil;
import com.bazaarvoice.emodb.stash.emr.sql.DocumentSchema;
import com.fasterxml.jackson.databind.util.ISO8601Utils;
import com.google.common.base.Charsets;
import com.google.common.base.Joiner;
import com.google.common.base.Throwables;
import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.hash.Hashing;
import com.google.common.io.Closeables;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.action.StoreTrueArgumentAction;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.Namespace;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.util.LongAccumulator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.Seq;
import scala.collection.Seq$;
import scala.collection.mutable.Builder;

import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.URI;
import java.text.ParsePosition;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.stream.Collectors;

import static com.bazaarvoice.emodb.stash.emr.generator.TableStatus.NA;
import static com.bazaarvoice.emodb.stash.emr.sql.DocumentSchema.toPollTime;
import static com.bazaarvoice.emodb.stash.emr.sql.DocumentSchema.toUpdateId;
import static com.google.common.base.Preconditions.checkArgument;

public class StashGenerator {

    private static final Logger _log = LoggerFactory.getLogger(StashGenerator.class);

    private static final DateTimeFormatter STASH_DIR_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss").withZone(ZoneOffset.UTC);
    public static final int DEFAULT_PARTITION_SIZE = 25000;
    public static final int DEFAULT_MAX_ASYNC_OPERATIONS = 2048;

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
        argParser.addArgument("--outputStashRoot")
                .help("Root Stash directory for writing.  Useful for testing (default is stashRoot value))");
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
                .required(true)
                .help("Spark master URL");
        argParser.addArgument("--region")
                .help("Region where S3 bucket is located if 'stashRoot' is in S3. (default is EC2 host's region)");
        argParser.addArgument("--existingTablesFile")
                .help("Location of a file which contains a list of existing Emo tables, one per line. " +
                      "Useful for local testing without an EmoDB service to query for the list.");
        argParser.addArgument("--noOptimizeUnmodifiedTables")
                .action(new StoreTrueArgumentAction())
                .help("Disable optimization for copying unmodified tables.  Useful for local testing.");
        argParser.addArgument("--partitionSize")
                .type(Integer.class)
                .setDefault(DEFAULT_PARTITION_SIZE)
                .help(String.format("Partition size for Stash gzip files (default is %d)", DEFAULT_PARTITION_SIZE));
        argParser.addArgument("--maxAsyncOperations")
                .type(Integer.class)
                .setDefault(DEFAULT_MAX_ASYNC_OPERATIONS)
                .help(String.format("Maximum number of concurrent async spark operations (default is %d)", DEFAULT_MAX_ASYNC_OPERATIONS));

        Namespace ns = argParser.parseArgs(args);

        String cluster = ns.getString("cluster");
        String apiKey = ns.getString("apikey");
        String databusSource = ns.getString("databusSource");
        URI stashRoot = URI.create(ns.getString("stashRoot"));
        URI outputStashRoot = URI.create(Optional.ofNullable(ns.getString("outputStashRoot")).or(ns.getString("stashRoot")));
        ZonedDateTime stashDate = Instant
                .ofEpochMilli(ISO8601Utils.parse(ns.getString("stashDate"), new ParsePosition(0)).getTime())
                .atZone(ZoneOffset.UTC);
        String master = ns.getString("master");
        String region = ns.getString("region");
        String existingTablesFile = ns.getString("existingTablesFile");
        boolean optimizeUnmodifiedTables = !ns.getBoolean("noOptimizeUnmodifiedTables");
        int partitionSize = ns.getInt("partitionSize");
        int maxAsyncOperations = ns.getInt("maxAsyncOperations");
        
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

        new StashGenerator().runStashGenerator(dataStore, databusSource, stashRoot, outputStashRoot, stashDate, master,
                Optional.fromNullable(region), Optional.fromNullable(existingTablesFile), optimizeUnmodifiedTables,
                partitionSize, maxAsyncOperations);
    }

    public void runStashGenerator(final DataStore dataStore, final String databusSource, final URI stashRoot,
                                  final URI outputStashRoot,
                                  final ZonedDateTime stashTime, final String master,
                                  Optional<String> region, Optional<String> existingTablesFile,
                                  boolean optimizeUnmodifiedTables, int partitionSize, int maxAsyncOperations) throws Exception {

        SparkSession sparkSession = SparkSession.builder()
                .appName("StashGenerator")
                .master(master)
                .getOrCreate();

        JavaSparkContext context = new JavaSparkContext(sparkSession.sparkContext());
        SQLContext sqlContext = SQLContext.getOrCreate(context.sc());
        StashReader priorStash = StashIO.getLatestStash(stashRoot, region);
        ZonedDateTime priorStashTime = STASH_DIR_FORMAT.parse(priorStash.getStashDirectory(), ZonedDateTime::from);

        String newStashDir = STASH_DIR_FORMAT.format(stashTime);
        StashWriter newStash = StashIO.createStash(outputStashRoot, newStashDir, region);

        checkArgument(priorStashTime.isBefore(stashTime), "Cannot create Stash older than existing latest Stash");
        _log.info("Creating Stash to {} merging with previous stash at {}", newStashDir, priorStash.getStashDirectory());
        
        // Get all tables that exist in Emo
        JavaPairRDD<String, Short> emoTables = getAllEmoTables(context, dataStore, existingTablesFile)
                .mapToPair(tableName -> new Tuple2<>(tableName, TableStatus.EXISTS_IN_EMO));

        // Get all tables that have been updated since the last stash
        JavaPairRDD<String, Short> updatedTables = getTablesWithDatabusUpdate(sqlContext, databusSource, priorStashTime, stashTime)
                .mapToPair(tableName -> new Tuple2<>(tableName, TableStatus.CONTAINS_UPDATES));

        JavaPairRDD<String, Short> allTables = emoTables.fullOuterJoin(updatedTables)
                .reduceByKey((left, right) -> new Tuple2<>(
                        Optional.of((short) (left._1.or(NA) | right._1.or(NA) | left._2.or(NA) | right._2.or(NA))), Optional.absent()))
                .mapValues(t -> (short) (t._1.or(NA) | t._2.or(NA)));

        // For those tables which exist and have no updates they can be copied as-is
        final JavaRDD<String> unmodifiedTables;
        final JavaRDD<String> mergeTables;

        if (optimizeUnmodifiedTables) {
            unmodifiedTables = allTables
                    .filter(t -> TableStatus.existsInEmo(t._2) && !TableStatus.containsUpdates(t._2))
                    .map(t -> t._1);

            mergeTables = allTables
                    .filter(t -> TableStatus.existsInEmo(t._2) && TableStatus.containsUpdates(t._2))
                    .map(t -> t._1);
        } else {
            unmodifiedTables = context.emptyRDD();
            mergeTables = allTables
                    .filter(t -> TableStatus.existsInEmo(t._2))
                    .map(t -> t._1);
        }

        JavaRDD<String> droppedTables = allTables
                .filter(t-> !TableStatus.existsInEmo(t._2))
                .map(t -> t._1)
                .sortBy(t -> t, true, 8);

        droppedTables.foreachPartitionAsync(tables -> {
            _log.info("The following tables contained updates since the last Stash but no longer exist in Emo: [{}]",
                    Joiner.on(",").join(tables));
        });

        ScheduledExecutorService monitorService = Executors.newScheduledThreadPool(2);
        ExecutorService opService = Executors.newCachedThreadPool();
        BroadcastRegistry broadcastRegistry = new BroadcastRegistry();
        try {
            AsyncOperations asyncOperations = new AsyncOperations(monitorService, opService, maxAsyncOperations);

            copyExistingStashTables(unmodifiedTables, priorStash, newStash, asyncOperations);

            copyAndMergeUpdatedStashTables(context, sqlContext, mergeTables, databusSource, priorStash, newStash,
                    priorStashTime, stashTime, partitionSize, asyncOperations, broadcastRegistry);

            asyncOperations.awaitAll();
        } finally {
            monitorService.shutdownNow();
            opService.shutdownNow();
            broadcastRegistry.destroyAll();
        }

        newStash.updateLatestFile();
    }

    private void copyExistingStashTables(final JavaRDD<String> tables, final StashReader priorStash,
                                         final StashWriter newStash, final AsyncOperations asyncOperations) {
        JavaRDD<Tuple2<String, String>> tableFiles = tables.flatMap(table ->
                priorStash.getTableFilesFromStash(table)
                        .stream()
                        .map(file -> new Tuple2<>(table, file))
                        .iterator());

        asyncOperations.watchAndThen(tableFiles.countAsync(), count -> {
            if (count != 0) {
                asyncOperations.watch(
                        tableFiles
                                .repartition((int) Math.ceil((float) count / 20))
                                .foreachAsync(t -> priorStash.copyTableFile(newStash, t._1, t._2)));
            }
        });
    }

    private void copyAndMergeUpdatedStashTables(final JavaSparkContext sparkContext, final SQLContext sqlContext,
                                                final JavaRDD<String> tablesRDD,
                                                final String databusSource,
                                                final StashReader priorStash, final StashWriter newStash,
                                                final ZonedDateTime priorStashTime, final ZonedDateTime stashTime,
                                                final int partitionSize,
                                                final AsyncOperations asyncOperations,
                                                final BroadcastRegistry broadcastRegistry) {

        // Get all tables which will be merged and assign each a unique integer ID.
        Broadcast<BiMap<String, Integer>> allTables;
        {
            Map<String, Integer> allTablesLocal = Maps.newHashMap();
            int tableIndex = 0;
            for (String table : tablesRDD.collect()) {
                allTablesLocal.put(table, tableIndex++);
            }
            allTables = broadcastRegistry.register(sparkContext.broadcast(ImmutableBiMap.copyOf(allTablesLocal)));
        }

        JavaRDD<Integer> tableIndexRDD = tablesRDD
                .map(table -> allTables.getValue().get(table));

        // Get all files which will be read from the prior Stash.  Use a temporary RDD to parallize reading the
        // numerous Stash tables.
        Broadcast<Map<StashFile, String>> priorStashFiles;
        {
            JavaPairRDD<StashFile, String> priorStashFilesRDD = tableIndexRDD
                    .flatMapToPair(tableIndex -> {
                        int fileIndex = 0;
                        List<String> tableNames = priorStash.getTableFilesFromStash(allTables.getValue().inverse().get(tableIndex));
                        List<Tuple2<StashFile, String>> tuples = Lists.newArrayListWithCapacity(tableNames.size());
                        for (String tableName : tableNames) {
                            tuples.add(new Tuple2<>(new StashFile(tableIndex, fileIndex++), tableName));
                        }
                        return tuples.iterator();
                    });

            priorStashFiles = broadcastRegistry.register(sparkContext.broadcast(priorStashFilesRDD.collectAsMap()));

            priorStashFilesRDD.unpersist(false);
        }

        // Get all documents that exist in Databus
        JavaPairRDD<StashDocument, UUID> documentsInDatabus = getDocumentsWithDatabusUpdate(sqlContext, databusSource, priorStashTime, stashTime, allTables);

        // Get all documents that exist in the prior Stash
        JavaPairRDD<StashDocument, StashLocation> documentsInPriorStash = getDocumentsFromPriorStash(sparkContext, priorStashFiles, priorStash, allTables);

        // Fully join the two to get the set of all documents
        JavaPairRDD<StashDocument, Tuple2<Optional<UUID>, Optional<StashLocation>>> allDocuments = documentsInDatabus.fullOuterJoin(documentsInPriorStash)
                .persist(StorageLevel.MEMORY_AND_DISK_SER_2());

        // Get the number of documents from databus and prior stash which will be written
        LongAccumulator unsortedDatabusOutputDocCount = new LongAccumulator();
        LongAccumulator unsortedPriorStashOutputDocCount = new LongAccumulator();
        sparkContext.sc().register(unsortedDatabusOutputDocCount);
        sparkContext.sc().register(unsortedPriorStashOutputDocCount);

        allDocuments.foreach(t -> {
            if (t._2._1.isPresent()) {
                unsortedDatabusOutputDocCount.add(1);
            } else {
                unsortedPriorStashOutputDocCount.add(1);
            }
        });

        if (unsortedDatabusOutputDocCount.sum() != 0) {
            // For all documents with any databus updates write them to Stash from databus parquet
            JavaRDD<TableAndValue<UUID>> unsortedDatabusOutputDocs = allDocuments
                    .filter(t -> t._2._1.isPresent())
                    .map(t -> new TableAndValue<>(t._1.getTableIndex(), t._2._1.get()));

            asyncOperations.watch(
                    getUpdatedDocumentsFromDatabus(unsortedDatabusOutputDocs, sqlContext, databusSource, priorStashTime, stashTime, allTables)
                            .sortBy(t -> t, true, (int) Math.ceil((float) unsortedDatabusOutputDocCount.sum() / partitionSize))
                            .foreachPartitionAsync(iter -> writeDatabusPartitionToStash(iter, newStash, allTables)));
        }

        if (unsortedPriorStashOutputDocCount.sum() != 0) {
            // For all documents from the prior Stash that are not updated write them to Stash
            JavaRDD<TableAndValue<StashLocation>> unsortedPriorStashOutputDocs = allDocuments
                    .filter(t -> !t._2._1.isPresent())
                    .map(t -> new TableAndValue<>(t._1.getTableIndex(), t._2._2.get()));

            asyncOperations.watch(
                    unsortedPriorStashOutputDocs
                            .sortBy(t -> t, true, (int) Math.ceil((float) unsortedPriorStashOutputDocCount.sum() / partitionSize))
                            .foreachPartitionAsync(iter -> writePriorStashPartitionToStash(iter, priorStash, newStash, allTables, priorStashFiles)));
        }
    }

    private JavaRDD<String> getAllEmoTables(JavaSparkContext context, DataStore dataStore, Optional<String> existingTablesFile) {
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

    private JavaRDD<String> getTablesWithDatabusUpdate(SQLContext sqlContext, String databusSource,
                                                       ZonedDateTime priorStashTime, ZonedDateTime stashTime) {

        final Seq<Object> pollDates = getPollDates(priorStashTime, stashTime);
        final Dataset<Row> dataset = sqlContext.read().schema(DocumentSchema.SCHEMA).parquet(databusSource);

        return dataset.select(dataset.col(DocumentSchema.TABLE))
                .where(dataset.col(DocumentSchema.POLL_DATE).isin(pollDates))
                .distinct()
                .map(value -> value.getString(0), Encoders.STRING())
                .toJavaRDD();
    }

    private JavaPairRDD<StashDocument, UUID> getDocumentsWithDatabusUpdate(
            final SQLContext sqlContext, final String databusSource, final ZonedDateTime priorStashTime, final ZonedDateTime stashTime,
            final Broadcast<BiMap<String, Integer>> allTables) {

        final Seq<Object> pollDates = getPollDates(priorStashTime, stashTime);
        final Dataset<Row> dataset = sqlContext.read().schema(DocumentSchema.SCHEMA).parquet(databusSource);

        return dataset
                .select(DocumentSchema.TABLE, DocumentSchema.KEY, DocumentSchema.VERSION, DocumentSchema.UPDATE_ID)
                .where(dataset.col(DocumentSchema.POLL_DATE).isin(pollDates))
                .groupByKey(row -> new StashDocument(allTables.getValue().get(row.getString(0)), row.getString(1)), Encoders.javaSerialization(StashDocument.class))
                .reduceGroups((left, right) -> left.getLong(2) > right.getLong(2) ? left : right)
                .map(t -> new Tuple2<>(t._1, toUpdateId(t._2.getString(3))), Encoders.tuple(Encoders.javaSerialization(StashDocument.class), Encoders.javaSerialization(UUID.class)))
                .toJavaRDD()
                .mapToPair(t -> t);
    }

    private JavaPairRDD<StashDocument, StashLocation> getDocumentsFromPriorStash(
            final JavaSparkContext context, final Broadcast<Map<StashFile, String>> priorStashFiles,
            final StashReader priorStash, final Broadcast<BiMap<String, Integer>> allTables) {

        List<Tuple2<StashFile, String>> priorStashFileShuffled = priorStashFiles.getValue()
                .entrySet()
                .stream()
                .map(entry -> new Tuple2<>(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

        Collections.shuffle(priorStashFileShuffled);
        
        return context
                .parallelizePairs(priorStashFileShuffled, (int) Math.ceil((float) priorStashFileShuffled.size() / 10))
                .flatMapToPair(t -> {
                    final Iterator<Tuple2<Integer, DocumentMetadata>> lines =
                            priorStash.readStashTableFileMetadata(allTables.getValue().inverse().get(t._1.getTableIndex()), t._2);
                    return Iterators.transform(lines, l -> new Tuple2<>(new StashDocument(t._1.getTableIndex(), l._2.getDocumentId().getKey()), new StashLocation(t._1.getFileIndex(), l._1)));
                })
                .reduceByKey((left, right) -> left.compareTo(right) < 0 ? left : right);
    }

    private JavaRDD<TableAndValue<String>> getUpdatedDocumentsFromDatabus(
            JavaRDD<TableAndValue<UUID>> updateIdByTables, SQLContext sqlContext, String databusSource, ZonedDateTime priorStashTime,
            ZonedDateTime stashTime, Broadcast<BiMap<String, Integer>> allTables) {

        JavaRDD<Row> rows = updateIdByTables
                .map(t -> new GenericRow(new Object[] { allTables.getValue().inverse().get(t.getTableIndex()), t.getValue().toString() }));

        final Dataset<Row> updatedIdsDataset =  sqlContext.createDataFrame(rows, DataTypes.createStructType(
                ImmutableList.of(
                        DataTypes.createStructField(DocumentSchema.TABLE, DataTypes.StringType, false),
                        DataTypes.createStructField(DocumentSchema.UPDATE_ID, DataTypes.StringType, false))));

        final Seq<Object> pollDates = getPollDates(priorStashTime, stashTime);
        final Dataset<Row> allDocsDataset = sqlContext.read().schema(DocumentSchema.SCHEMA).parquet(databusSource);

        Seq<String> joinColumns = Seq$.MODULE$.<String>newBuilder()
                .$plus$eq(DocumentSchema.TABLE)
                .$plus$eq(DocumentSchema.UPDATE_ID)
                .result();

        Dataset<Row> joinedDocsDataset =  allDocsDataset.as("a").join(updatedIdsDataset.as("u"), joinColumns);

        return joinedDocsDataset
                .select(DocumentSchema.TABLE, DocumentSchema.JSON)
                .where(joinedDocsDataset.col(DocumentSchema.POLL_DATE).isin(pollDates)
                        .and(joinedDocsDataset.col(DocumentSchema.DELETED).equalTo(false)))
                .toJavaRDD()
                .map(row -> new TableAndValue<>(allTables.getValue().get(row.getString(0)), row.getString(1)));
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

    private static void writeDatabusPartitionToStash(Iterator<TableAndValue<String>> iter, StashWriter stashWriter,
                                                     Broadcast<BiMap<String, Integer>> allTables) {
        StashFileWriter tableWriter = null;
        int lastTableIndex = Integer.MIN_VALUE;

        try {
            while (iter.hasNext()) {
                TableAndValue<String> tableAndValue = iter.next();
                int tableIndex = tableAndValue.getTableIndex();
                String jsonLine = tableAndValue.getValue();

                if (tableIndex != lastTableIndex) {
                    if (tableWriter != null) {
                        tableWriter.close();
                    }
                    lastTableIndex = tableIndex;

                    // Use the hash from the first document to uniquely name the file
                    String key = JsonUtil.parseJson(jsonLine, DocumentMetadata.class).getDocumentId().getKey();
                    String suffix = Hashing.md5().hashString(key, Charsets.UTF_8).toString();

                    String table = allTables.getValue().inverse().get(tableIndex);
                    tableWriter = stashWriter.writeStashTableFile(table, suffix);
                }

                tableWriter.writeJsonLine(jsonLine);
            }

            if (tableWriter != null) {
                tableWriter.close();
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private static void writePriorStashPartitionToStash(
            Iterator<TableAndValue<StashLocation>> iter, StashReader stashReader, StashWriter stashWriter,
            Broadcast<BiMap<String, Integer>> allTables, Broadcast<Map<StashFile, String>> priorStashFiles) {

        StashFileWriter tableWriter = null;
        int lastTableIndex = Integer.MIN_VALUE;
        int lastFileIndex = Integer.MIN_VALUE;
        String table = null;
        CloseableIterator<Tuple2<Integer, String>> stashFileJson = null;

        try {
            while (iter.hasNext()) {
                TableAndValue<StashLocation> tableAndValue = iter.next();
                int tableIndex = tableAndValue.getTableIndex();
                StashLocation loc = tableAndValue.getValue();

                if (tableIndex != lastTableIndex) {
                    if (tableWriter != null) {
                        tableWriter.close();
                        tableWriter = null;
                    }
                    lastTableIndex = tableIndex;
                    lastFileIndex = Integer.MIN_VALUE;
                    table = allTables.getValue().inverse().get(tableIndex);
                }

                int currentFileIndex = loc.getFileIndex();
                int currentLineNum = loc.getLine();

                if (currentFileIndex != lastFileIndex) {
                    if (stashFileJson != null) {
                        Closeables.close(stashFileJson, true);
                    }
                    lastFileIndex = currentFileIndex;
                    stashFileJson = stashReader.readStashTableFileJson(table, priorStashFiles.getValue().get(new StashFile(tableIndex, currentFileIndex)));
                }

                String jsonLine = forwardToLine(stashFileJson, currentLineNum);

                if (tableWriter == null) {
                    // Use the hash from the first document to uniquely name the file
                    String key = JsonUtil.parseJson(jsonLine, DocumentMetadata.class).getDocumentId().getKey();
                    String suffix = Hashing.md5().hashString(key, Charsets.UTF_8).toString();
    
                    tableWriter = stashWriter.writeStashTableFile(table, suffix);
                }

                tableWriter.writeJsonLine(jsonLine);
            }

            if (tableWriter != null) {
                tableWriter.close();
            }
            Closeables.close(stashFileJson, true);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private static String forwardToLine(Iterator<Tuple2<Integer, String>> jsonLines, int lineNum) {
        while (jsonLines.hasNext()) {
            Tuple2<Integer, String> jsonLine = jsonLines.next();
            if (jsonLine._1 == lineNum) {
                return jsonLine._2;
            }
        }
        throw new IllegalStateException("Line not found");
    }
}
