package com.bazaarvoice.emodb.stash.emr.generator;

import com.bazaarvoice.emodb.stash.emr.DocumentMetadata;
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
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.hash.Hashing;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.impl.action.StoreTrueArgumentAction;
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
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.storage.StorageLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.Seq;
import scala.collection.Seq$;
import scala.collection.mutable.Builder;

import javax.annotation.Nullable;
import javax.ws.rs.core.UriBuilder;
import java.io.IOException;
import java.net.URI;
import java.text.ParsePosition;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;

import static com.bazaarvoice.emodb.stash.emr.generator.TableStatus.NA;
import static com.bazaarvoice.emodb.stash.emr.sql.DocumentSchema.toPollTime;
import static com.bazaarvoice.emodb.stash.emr.sql.DocumentSchema.toUpdateId;
import static com.google.common.base.Preconditions.checkArgument;

public class StashGenerator {

    private static final Logger _log = LoggerFactory.getLogger(StashGenerator.class);

    private static final DateTimeFormatter STASH_DIR_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss").withZone(ZoneOffset.UTC);
    public static final int DEFAULT_PARTITION_SIZE = 25000;

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

        DataStore dataStore = new DataStore(dataStoreDiscoveryBuilder, apiKey);

        new StashGenerator().runStashGenerator(dataStore, databusSource, stashRoot, outputStashRoot, stashDate, master,
                Optional.fromNullable(region), Optional.fromNullable(existingTablesFile), optimizeUnmodifiedTables,
                partitionSize);
    }

    public void runStashGenerator(final DataStore dataStore, final String databusSource, final URI stashRoot,
                                  final URI outputStashRoot,
                                  final ZonedDateTime stashTime, @Nullable final String master,
                                  Optional<String> region, Optional<String> existingTablesFile,
                                  boolean optimizeUnmodifiedTables, int partitionSize) throws Exception {

        SparkConf sparkConf = new SparkConf().setAppName("StashGenerator");
        if (master != null) {
            sparkConf.setMaster(master);
        }

        JavaSparkContext context = new JavaSparkContext(sparkConf);
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

        ScheduledExecutorService service = Executors.newScheduledThreadPool(2);
        try {
            AsyncOperations asyncOperations = new AsyncOperations(service);

            copyExistingStashTables(unmodifiedTables, priorStash, newStash, asyncOperations);

            copyAndMergeUpdatedStashTables(context, sqlContext, mergeTables, databusSource, priorStash, newStash,
                    priorStashTime, stashTime, partitionSize, asyncOperations);

            asyncOperations.awaitAll();
        } finally {
            service.shutdownNow();
        }

        newStash.updateLatestFile();
    }

    private void copyExistingStashTables(final JavaRDD<String> tables, final StashReader priorStash,
                                         final StashWriter newStash, final AsyncOperations asyncOperations) {
        JavaRDD<StashFile> tableFiles = tables.flatMap(table ->
                priorStash.getTableFilesFromStash(table)
                        .stream()
                        .map(file -> new StashFile(table, file))
                        .iterator());

        asyncOperations.watchAndThen(tableFiles.countAsync(), count -> {
            if (count != 0) {
                JavaFutureAction<Void> future = tableFiles
                        .repartition((int) Math.ceil((float) count / 20))
                        .foreachAsync(stashFile -> priorStash.copyTableFile(newStash, stashFile.getTable(), stashFile.getFile()));

                asyncOperations.watch(future);
            }
        });
    }

    private void copyAndMergeUpdatedStashTables(final JavaSparkContext sparkContext, final SQLContext sqlContext,
                                                final JavaRDD<String> tablesRDD,
                                                final String databusSource,
                                                final StashReader priorStash, final StashWriter newStash,
                                                final ZonedDateTime priorStashTime, final ZonedDateTime stashTime,
                                                final int partitionSize,
                                                final AsyncOperations asyncOperations) {

        List<String> tables = tablesRDD.collect();
        List<Future<Void>> futures = Lists.newArrayListWithCapacity(tables.size() * 2);

        for (final String table : tables) {
            // Get all documents that exist in Databus
            JavaPairRDD<String, UUID> documentsInDatabus = getDocumentsWithDatabusUpdate(sqlContext, table, databusSource, priorStashTime, stashTime);

            // Get all documents that exist in the prior Stash
            JavaPairRDD<String, StashLocation> documentsInPriorStash = getDocumentsFromPriorStash(sparkContext, table, priorStash);

            // Fully join the two to get the set of all documents
            JavaPairRDD<String, Tuple2<Optional<UUID>, Optional<StashLocation>>> allDocuments = documentsInDatabus.fullOuterJoin(documentsInPriorStash);

            // For all documents with any databus updates write them to Stash from databus parquet
            JavaRDD<UUID> unsortedDatabusOutputDocs =
                    allDocuments.filter(t -> t._2._1.isPresent())
                            .map(t -> t._2._1.get());

            asyncOperations.watchAndThen(unsortedDatabusOutputDocs.countAsync(), count -> {
                if (count != 0) {
                    JavaFutureAction<Void> databusFuture = getUpdatedDocumentsFromDatabus(table, unsortedDatabusOutputDocs, sqlContext, databusSource, priorStashTime, stashTime)
                            .toJavaRDD()
                            .repartition((int) Math.ceil((float) count / partitionSize))
                            .foreachPartitionAsync(iter -> writeDatabusPartitionToStash(iter, table, newStash));

                    asyncOperations.watch(databusFuture);
                }
            });

            // For all documents from the prior Stash that are not updated write them to Stash
            JavaRDD<StashLocation> unsortedPriorStashOutputDocs = allDocuments
                    .filter(t -> !t._2._1.isPresent())
                    .map(t -> t._2._2.get());

            asyncOperations.watchAndThen(unsortedPriorStashOutputDocs.countAsync(), count -> {
                if (count != 0) {
                    JavaFutureAction<Void> stashFuture = unsortedPriorStashOutputDocs
                            .sortBy(t -> t, true, (int) Math.ceil((float) count / partitionSize))
                            .foreachPartitionAsync(iter -> writePriorStashPartitionToStash(iter, table, priorStash, newStash));

                    asyncOperations.watch(stashFuture);
                }
            });
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

    private JavaPairRDD<String, UUID> getDocumentsWithDatabusUpdate(
            SQLContext sqlContext, String table, String databusSource, ZonedDateTime priorStashTime, ZonedDateTime stashTime) {
        final Seq<Object> pollDates = getPollDates(priorStashTime, stashTime);
        final Dataset<Row> dataset = sqlContext.read().schema(DocumentSchema.SCHEMA).parquet(databusSource);

        return dataset
                .select(DocumentSchema.KEY, DocumentSchema.VERSION, DocumentSchema.UPDATE_ID)
                .where(dataset.col(DocumentSchema.POLL_DATE).isin(pollDates)
                        .and(dataset.col(DocumentSchema.TABLE).equalTo(table)))
                .groupByKey(row -> row.getString(0), Encoders.STRING())
                .reduceGroups((left, right) -> left.getLong(1) > right.getLong(1) ? left : right)
                .map(t -> new Tuple2<>(t._1, toUpdateId(t._2.getString(2))), Encoders.tuple(Encoders.STRING(), Encoders.javaSerialization(UUID.class)))
                .toJavaRDD()
                .mapToPair(t -> t);
    }

    private JavaPairRDD<String, StashLocation> getDocumentsFromPriorStash(final JavaSparkContext sparkContext, final String table, final StashReader priorStash) {
        List<String> priorStashFilesNames = priorStash.getTableFilesFromStash(table);

        if (priorStashFilesNames.isEmpty()) {
            return sparkContext.emptyRDD().flatMapToPair(t -> Iterators.emptyIterator());
        }

        JavaRDD<String> priorStashFiles = sparkContext.parallelize(priorStashFilesNames, (int) Math.ceil((float) priorStashFilesNames.size() / 4));
        
        return priorStashFiles
                .flatMapToPair(file -> {
                    final Iterator<Tuple2<Integer, DocumentMetadata>> lines = priorStash.readStashTableFileMetadata(table, file);
                    return Iterators.transform(lines, t -> new Tuple2<>(t._2.getDocumentId().getKey(), new StashLocation(file, t._1)));
                })
                .reduceByKey((left, right) -> left.compareTo(right) < 0 ? left : right)
                .persist(StorageLevel.MEMORY_AND_DISK_SER_2());
    }

    private Dataset<String> getUpdatedDocumentsFromDatabus(
            String table, JavaRDD<UUID> updateIds, SQLContext sqlContext, String databusSource, ZonedDateTime priorStashTime, ZonedDateTime stashTime) {

        JavaRDD<Row> rows = updateIds
                .map(uuid -> new GenericRow(new Object[] { uuid.toString() }));

        final Dataset<Row> updatedIdsDataset =  sqlContext.createDataFrame(rows, DataTypes.createStructType(
                ImmutableList.of(DataTypes.createStructField(DocumentSchema.UPDATE_ID, DataTypes.StringType, false))));

        final Seq<Object> pollDates = getPollDates(priorStashTime, stashTime);
        final Dataset<Row> allDocsDataset = sqlContext.read().schema(DocumentSchema.SCHEMA).parquet(databusSource);

        Dataset<Row> joinedDocsdataset =  allDocsDataset.as("a").join(updatedIdsDataset.as("u"), DocumentSchema.UPDATE_ID);

        return joinedDocsdataset
                .select(DocumentSchema.JSON)
                .where(joinedDocsdataset.col(DocumentSchema.POLL_DATE).isin(pollDates)
                        .and(joinedDocsdataset.col(DocumentSchema.TABLE).equalTo(table))
                        .and(joinedDocsdataset.col(DocumentSchema.DELETED).equalTo(false)))
                .map(row -> row.getString(0), Encoders.STRING());
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

    private static void writeDatabusPartitionToStash(Iterator<String> iter, String table, StashWriter stashWriter) {
        if (iter.hasNext()) {
            String jsonLine = iter.next();

            // Use the hash from the first document to uniquely name the file
            String key = JsonUtil.parseJson(jsonLine, DocumentMetadata.class).getDocumentId().getKey();
            String suffix = Hashing.md5().hashString(key, Charsets.UTF_8).toString();

            try (StashFileWriter tableWriter = stashWriter.writeStashTableFile(table, suffix)) {
                tableWriter.writeJsonLine(jsonLine);

                while (iter.hasNext()) {
                    jsonLine = iter.next();
                    tableWriter.writeJsonLine(jsonLine);
                }
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
        }
    }

    private static void writePriorStashPartitionToStash(Iterator<StashLocation> iter, String table, StashReader stashReader, StashWriter stashWriter) {
        if (iter.hasNext()) {
            StashLocation loc = iter.next();
            String currentFile = loc.getFile();
            int currentLineNum = loc.getLine();
            Iterator<Tuple2<Integer, String>> stashFileJson = stashReader.readStashTableFileJson(table, currentFile);

            // Move to the first line
            String jsonLine = forwardToLine(stashFileJson, currentLineNum);

            // Use the hash from the first document to uniquely name the file
            String key = JsonUtil.parseJson(jsonLine, DocumentMetadata.class).getDocumentId().getKey();
            String suffix = Hashing.md5().hashString(key, Charsets.UTF_8).toString();

            try (StashFileWriter tableWriter = stashWriter.writeStashTableFile(table, suffix)) {
                tableWriter.writeJsonLine(jsonLine);

                while (iter.hasNext()) {
                    loc = iter.next();
                    if (!loc.getFile().equals(currentFile)) {
                        currentFile = loc.getFile();
                        stashFileJson = stashReader.readStashTableFileJson(table, currentFile);
                    }
                    currentLineNum = loc.getLine();
                    jsonLine = forwardToLine(stashFileJson, currentLineNum);
                    tableWriter.writeJsonLine(jsonLine);
                }
            } catch (IOException e) {
                throw Throwables.propagate(e);
            }
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
