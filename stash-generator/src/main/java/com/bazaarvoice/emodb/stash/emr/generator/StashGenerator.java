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

import static com.bazaarvoice.emodb.stash.emr.generator.TableStatus.combine;
import static com.bazaarvoice.emodb.stash.emr.sql.DocumentSchema.toPollTime;
import static com.bazaarvoice.emodb.stash.emr.sql.DocumentSchema.toUpdateId;
import static com.google.common.base.Preconditions.checkArgument;

/**
 * Spark job for generating a new Stash using the previous Stash and a table of updates since that Stash generated
 * by the DatabusAccumulator.
 */
public class StashGenerator {

    private static final Logger _log = LoggerFactory.getLogger(StashGenerator.class);

    private static final DateTimeFormatter STASH_DIR_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss").withZone(ZoneOffset.UTC);
    public static final int DEFAULT_PARTITION_SIZE = 100000;

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

        try (DataStore dataStore = new DataStore(dataStoreDiscoveryBuilder, apiKey)) {
            new StashGenerator().runStashGenerator(dataStore, databusSource, stashRoot, outputStashRoot, stashDate, master,
                    Optional.fromNullable(region), Optional.fromNullable(existingTablesFile), optimizeUnmodifiedTables,
                    partitionSize);
        }
    }

    public void runStashGenerator(final DataStore dataStore, final String databusSource, final URI stashRoot,
                                  final URI outputStashRoot,
                                  final ZonedDateTime stashTime, final String master,
                                  Optional<String> region, Optional<String> existingTablesFile,
                                  boolean optimizeUnmodifiedTables, int partitionSize) throws Exception {

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

        // Combine the two for a full list of potential Emo tables
        JavaPairRDD<String, Short> allTables = emoTables.fullOuterJoin(updatedTables)
                .reduceByKey((left, right) -> new Tuple2<>(Optional.of(combine(left._1, left._2, right._1, right._2)), Optional.absent()))
                .mapValues(t -> combine(t._1, t._2));

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

        // The databus may have updates for documents in tables which have been dropped.  Log that those tables
        // will not be written to the new Stash.
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
            AsyncOperations asyncOperations = new AsyncOperations(monitorService, opService);

            // Efficiently copy unmodified tables by performing a file copy from the prior to new Stash.
            copyExistingStashTables(unmodifiedTables, priorStash, newStash, asyncOperations);

            // Merge modified documents from the databus with unmodified documents from the prior Stash to the new Stash.
            copyAndMergeUpdatedStashTables(context, sqlContext, mergeTables, databusSource, priorStash, newStash,
                    priorStashTime, stashTime, partitionSize, asyncOperations, broadcastRegistry);

            asyncOperations.awaitAll();
        } finally {
            monitorService.shutdownNow();
            opService.shutdownNow();
            broadcastRegistry.destroyAll();
        }

        // Stash generation complete; update the _LATEST file for the new Stash.
        newStash.updateLatestFile();
    }

    /**
     * Adds Spark workflow for efficiently copying unmodified tables from the prior Stash to the new Stash.
     */
    private void copyExistingStashTables(final JavaRDD<String> tables, final StashReader priorStash,
                                         final StashWriter newStash, final AsyncOperations asyncOperations) {
        // Create an RDD of all of the files which will be copied.
        JavaRDD<Tuple2<String, String>> tableFiles = tables.flatMap(table ->
                priorStash.getTableFilesFromStash(table)
                        .stream()
                        .map(file -> new Tuple2<>(table, file))
                        .iterator());

        // Attempt to create partitions with 20 files per partition.  Large partitions could lead to re-copying many files
        // if there is an error copying an individual file.  This partitioning reduces the amount of redundant work performed
        // in case of a task failure.
        asyncOperations.watchAndThen(tableFiles.countAsync(), count -> {
            if (count != 0) {
                asyncOperations.watch(
                        tableFiles
                                .repartition((int) Math.ceil((float) count / 20))
                                .foreachAsync(t -> priorStash.copyTableFile(newStash, t._1, t._2)));
            }
        });
    }

    /**
     * Adds Spark workflows for merging records from the databus updates and prior Stash to generate new Stash files.
     */
    private void copyAndMergeUpdatedStashTables(final JavaSparkContext sparkContext, final SQLContext sqlContext,
                                                final JavaRDD<String> tablesRDD,
                                                final String databusSource,
                                                final StashReader priorStash, final StashWriter newStash,
                                                final ZonedDateTime priorStashTime, final ZonedDateTime stashTime,
                                                final int partitionSize,
                                                final AsyncOperations asyncOperations,
                                                final BroadcastRegistry broadcastRegistry) {

        // Get all tables which will be merged and assign each a unique integer ID.  Future RDDs can then refer to the
        // tables by ID to reduce memory utilization.
        Broadcast<BiMap<String, Integer>> allTables;
        {
            Map<String, Integer> allTablesLocal = Maps.newHashMap();
            int tableIndex = 0;
            for (String table : tablesRDD.collect()) {
                allTablesLocal.put(table, tableIndex++);
            }
            allTables = broadcastRegistry.register(sparkContext.broadcast(ImmutableBiMap.copyOf(allTablesLocal)));
        }

        // Get all files which will be read from the prior Stash.  Assign each file a unique integer ID within the scope
        // of the table.  Future RDDs can then refer to the file by ID to reduce memory utilization.
        Broadcast<Map<StashFile, String>> priorStashFiles;
        {
            JavaPairRDD<StashFile, String> priorStashFilesRDD = tablesRDD
                    .flatMapToPair(table -> {
                        int tableIndex = allTables.getValue().get(table);
                        int fileIndex = 0;
                        List<String> fileNames = priorStash.getTableFilesFromStash(table);
                        List<Tuple2<StashFile, String>> tuples = Lists.newArrayListWithCapacity(fileNames.size());
                        for (String fileName : fileNames) {
                            tuples.add(new Tuple2<>(new StashFile(tableIndex, fileIndex++), fileName));
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

        // Get the number of documents from databus and prior stash which will be written.  This will be used create
        // partitions with sizes close to <code>partitionSize</code>.
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
            JavaRDD<TableAndValue<UUID>> updateIdByTable = allDocuments
                    .filter(t -> t._2._1.isPresent())
                    .map(t -> new TableAndValue<>(t._1.getTableIndex(), t._2._1.get()));

            asyncOperations.watch(
                    getUpdatedDocumentsFromDatabus(updateIdByTable, sqlContext, databusSource, priorStashTime, stashTime, allTables)
                            .sortBy(t -> t, true, (int) Math.ceil((float) unsortedDatabusOutputDocCount.sum() / partitionSize))
                            .foreachPartitionAsync(iter -> writeDatabusPartitionToStash(iter, newStash, allTables)));
        }

        if (unsortedPriorStashOutputDocCount.sum() != 0) {
            // For all documents from the prior Stash that are not updated write them to Stash
            JavaRDD<TableAndValue<StashLocation>> locationByTable = allDocuments
                    .filter(t -> !t._2._1.isPresent())
                    .map(t -> new TableAndValue<>(t._1.getTableIndex(), t._2._2.get()));

            asyncOperations.watch(
                    locationByTable
                            .sortBy(t -> t, true, (int) Math.ceil((float) unsortedPriorStashOutputDocCount.sum() / partitionSize))
                            .foreachPartitionAsync(iter -> writePriorStashPartitionToStash(iter, priorStash, newStash, allTables, priorStashFiles)));
        }
    }

    /**
     * Creates an RDD with the names of all available tables in EmoDB.
     */
    private JavaRDD<String> getAllEmoTables(JavaSparkContext context, DataStore dataStore, Optional<String> existingTablesFile) {
        if (existingTablesFile.isPresent()) {
            // A file was explicitly provided with the full list of EmoDB tables.
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

    /**
     * Creates an RDD with the names of all tables which have at at least one updated document from the databus.
     */
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

    /**
     * Creates an RDD of all documents with updates from the databus mapped to the UUID of the databus update.
     */
    private JavaPairRDD<StashDocument, UUID> getDocumentsWithDatabusUpdate(
            final SQLContext sqlContext, final String databusSource, final ZonedDateTime priorStashTime, final ZonedDateTime stashTime,
            final Broadcast<BiMap<String, Integer>> allTables) {

        final Seq<Object> pollDates = getPollDates(priorStashTime, stashTime);
        final Dataset<Row> dataset = sqlContext.read().schema(DocumentSchema.SCHEMA).parquet(databusSource);

        return dataset
                .select(DocumentSchema.TABLE, DocumentSchema.KEY, DocumentSchema.VERSION, DocumentSchema.UPDATE_ID)
                .where(dataset.col(DocumentSchema.POLL_DATE).isin(pollDates))
                .groupByKey(row -> new StashDocument(allTables.getValue().get(row.getString(0)), row.getString(1)), Encoders.javaSerialization(StashDocument.class))
                // If the document was updated more than once keep the one with the highest version.
                // Two documents with the same version should be identical so arbitrarily keep either one.
                .reduceGroups((left, right) -> left.getLong(2) > right.getLong(2) ? left : right)
                .map(t -> new Tuple2<>(t._1, toUpdateId(t._2.getString(3))), Encoders.tuple(Encoders.javaSerialization(StashDocument.class), Encoders.javaSerialization(UUID.class)))
                .toJavaRDD()
                .mapToPair(t -> t);
    }

    /**
     * Creates an RDD of all documents which existed in the prior Stash mapped to the location of the JSON content.
     */
    private JavaPairRDD<StashDocument, StashLocation> getDocumentsFromPriorStash(
            final JavaSparkContext context, final Broadcast<Map<StashFile, String>> priorStashFiles,
            final StashReader priorStash, final Broadcast<BiMap<String, Integer>> allTables) {

        // Shuffle the list of Stash files to reduce the chances of having clumps of large files.
        List<Tuple2<StashFile, String>> priorStashFileShuffled = priorStashFiles.getValue()
                .entrySet()
                .stream()
                .map(entry -> new Tuple2<>(entry.getKey(), entry.getValue()))
                .collect(Collectors.toList());

        Collections.shuffle(priorStashFileShuffled);
        
        return context
                // Group the files into roughly 10 files per partitions
                .parallelizePairs(priorStashFileShuffled, (int) Math.ceil((float) priorStashFileShuffled.size() / 10))
                // Read each file and expand the documents contained in the file
                .flatMapToPair(t -> {
                    final Iterator<Tuple2<Integer, DocumentMetadata>> lines =
                            priorStash.readStashTableFileMetadata(allTables.getValue().inverse().get(t._1.getTableIndex()), t._2);
                    return Iterators.transform(lines, l -> new Tuple2<>(new StashDocument(t._1.getTableIndex(), l._2.getDocumentId().getKey()), new StashLocation(t._1.getFileIndex(), l._1)));
                })
                // On the odd chance that a single Stash contains the same document more than once arbitrarily keep the
                // first one encountered.  This normally shouldn't happen but is possible under specific conditions.
                .reduceByKey((left, right) -> left.compareTo(right) < 0 ? left : right);
    }

    /**
     * Creates an RDD of the actual JSON documents which need to be written from the databus.
     */
    private JavaRDD<TableAndValue<EncodedStashLine>> getUpdatedDocumentsFromDatabus(
            JavaRDD<TableAndValue<UUID>> updateIdByTables, SQLContext sqlContext, String databusSource, ZonedDateTime priorStashTime,
            ZonedDateTime stashTime, Broadcast<BiMap<String, Integer>> allTables) {

        // Since we need to join with a parquet table convert the updated documents into an RDD of Rows.
        JavaRDD<Row> rows = updateIdByTables
                .map(t -> new GenericRow(new Object[] { allTables.getValue().inverse().get(t.getTableIndex()), t.getValue().toString() }));

        final Dataset<Row> updatedIdsDataset =  sqlContext.createDataFrame(rows, DataTypes.createStructType(
                ImmutableList.of(
                        DataTypes.createStructField(DocumentSchema.TABLE, DataTypes.StringType, false),
                        DataTypes.createStructField(DocumentSchema.UPDATE_ID, DataTypes.StringType, false))));

        final Dataset<Row> allDocsDataset = sqlContext.read().schema(DocumentSchema.SCHEMA).parquet(databusSource);

        final Seq<Object> pollDates = getPollDates(priorStashTime, stashTime);

        Seq<String> joinColumns = Seq$.MODULE$.<String>newBuilder()
                .$plus$eq(DocumentSchema.TABLE)
                .$plus$eq(DocumentSchema.UPDATE_ID)
                .result();

        Dataset<Row> joinedDocsDataset =  allDocsDataset
                .where(allDocsDataset.col(DocumentSchema.POLL_DATE).isin(pollDates))
                .as("a")
                .join(updatedIdsDataset.as("u"), joinColumns);

        return joinedDocsDataset
                .select(DocumentSchema.TABLE, DocumentSchema.ENCODING, DocumentSchema.CONTENT)
                .where(joinedDocsDataset.col(DocumentSchema.DELETED).equalTo(false))
                .toJavaRDD()
                .map(row -> new TableAndValue<>(allTables.getValue().get(row.getString(0)), new EncodedStashLine(row.getInt(1), row.getAs(2))));
    }

    /**
     * Given a prior Stash time and the new Stash time, returns a sequence of all parquet {@link DocumentSchema#POLL_DATE}
     * values which all between the two times.
     */
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

    /**
     * Writes a partition of databus documents to Stash files.  The partition should be sorted by table to optimize
     * writes and minimize the number of Stash files created.
     */
    private static void writeDatabusPartitionToStash(Iterator<TableAndValue<EncodedStashLine>> iter, StashWriter stashWriter,
                                                     Broadcast<BiMap<String, Integer>> allTables) {
        StashFileWriter tableWriter = null;
        int lastTableIndex = Integer.MIN_VALUE;

        try {
            while (iter.hasNext()) {
                TableAndValue<EncodedStashLine> tableAndValue = iter.next();
                int tableIndex = tableAndValue.getTableIndex();
                String jsonLine = tableAndValue.getValue().getJson();

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
                tableWriter = null;
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        } finally {
            try {
                Closeables.close(tableWriter, true);
            } catch (IOException e2) {
                // Already managed
            }
        }
    }

    /**
     * Writes a partition of prior Stash documents to Stash files.  The partition should be sorted by table
     * and location to minimize the number of IO operations from the prior Stash.
     */
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
                tableWriter = null;
            }
            Closeables.close(stashFileJson, true);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        } finally {
            try {
                Closeables.close(tableWriter, true);
            } catch (IOException e2) {
                // Already managed
            }
        }
    }

    /**
     * Moves an iterator of JSON lines forward to the matching line.  The iterator must provide lines numbers
     * in increasing order.
     */
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
