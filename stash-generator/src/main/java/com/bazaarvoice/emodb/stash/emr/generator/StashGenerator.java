package com.bazaarvoice.emodb.stash.emr.generator;

import com.bazaarvoice.emodb.stash.emr.DocumentId;
import com.bazaarvoice.emodb.stash.emr.DocumentMetadata;
import com.bazaarvoice.emodb.stash.emr.generator.io.StashIO;
import com.bazaarvoice.emodb.stash.emr.generator.io.StashReader;
import com.bazaarvoice.emodb.stash.emr.generator.io.StashWriter;
import com.bazaarvoice.emodb.stash.emr.sql.DocumentSchema;
import com.fasterxml.jackson.databind.util.ISO8601Utils;
import com.google.common.base.Joiner;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;
import com.google.common.collect.PeekingIterator;
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
import java.util.Iterator;
import java.util.List;

import static com.bazaarvoice.emodb.stash.emr.generator.TableStatus.NA;
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
                .setDefault(DocumentPartitioner.DEFAULT_PARTITION_SIZE)
                .help(String.format("Partition size for Stash gzip files (default is %d)", DocumentPartitioner.DEFAULT_PARTITION_SIZE));

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
        
        // Get all tables that exist in Stash
        JavaPairRDD<String, Short> emoTables = getEmoTableNamesRDD(context, dataStore, existingTablesFile)
                .mapToPair(tableName -> new Tuple2<>(tableName, TableStatus.EXISTS_IN_EMO));

        // Get all tables that have been updated since the last stash
        JavaPairRDD<String, Short> updatedTables = getDatabusEventsTablesRDD(sqlContext, databusSource, priorStashTime, stashTime)
                .mapToPair(tableName -> new Tuple2<>(tableName, TableStatus.CONTAINS_UPDATES));

        // Map all tables that exist in Emo with any that may have received updates since the last Stash
        JavaPairRDD<String, Short> allTables = emoTables.fullOuterJoin(updatedTables)
                .reduceByKey((left, right) -> new Tuple2<>(
                        Optional.of((short) (left._1.or(NA) | right._1.or(NA) | left._2.or(NA) | right._2.or(NA))), Optional.absent()))
                .mapValues(t -> (short) (t._1.or(NA) | t._2.or(NA)))
                .persist(StorageLevel.MEMORY_AND_DISK_SER_2());

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

        List<JavaFutureAction<Void>> futures = Lists.newArrayListWithCapacity(2);

        futures.add(copyExistingStashTables(unmodifiedTables, priorStash, newStash));

        futures.add(copyAndMergeUpdatedStashTables(sqlContext, mergeTables, databusSource, priorStash, newStash, priorStashTime, stashTime, partitionSize));

        for (JavaFutureAction<Void> future : futures) {
            future.get();
        }
    }

    private JavaFutureAction<Void> copyExistingStashTables(final JavaRDD<String> tables, final StashReader priorStash,
                                                           final StashWriter newStash) {
        JavaPairRDD<String, String> tableFiles = tables.flatMapToPair(table ->
                priorStash.getTableFilesFromStash(table)
                        .stream()
                        .map(file -> new Tuple2<>(table, file))
                        .iterator());

        return tableFiles.foreachAsync(t -> priorStash.copyTableFile(newStash, t._1, t._2));
    }

    private JavaFutureAction<Void> copyAndMergeUpdatedStashTables(final SQLContext sqlContext, final JavaRDD<String> tables,
                                                                  final String databusSource,
                                                                  final StashReader priorStash, final StashWriter newStash,
                                                                  final ZonedDateTime priorStashTime, final ZonedDateTime stashTime,
                                                                  final int partitionSize) {

        // Get all updated documents

        JavaPairRDD<DocumentId, DocumentLocation> databusDocs =
                getDocumentsWithDatabusUpdate(sqlContext, databusSource, priorStashTime, stashTime);

        // Get all documents in Stash

        JavaPairRDD<String, String> existingTableFiles = tables.flatMapToPair(table ->
                priorStash.getTableFilesFromStash(table)
                        .stream()
                        .map(file -> new Tuple2<>(table, file))
                        .iterator());

        JavaPairRDD<DocumentId, DocumentLocation> priorStashDocs = existingTableFiles
                .flatMapToPair(t -> {
                    final String table = t._1;
                    final String file = t._2;
                    final Iterator<Tuple2<Integer, DocumentMetadata>> lines = priorStash.readStashTableFileMetadata(table, file);
                    return Iterators.transform(lines, e -> new Tuple2<>(
                            e._2.getDocumentId(), DocumentLocation.stash(table, file, e._1)));
                });

        JavaPairRDD<DocumentLocation, Boolean> allDocsByTable = databusDocs.fullOuterJoin(priorStashDocs)
                .mapValues(t -> t._1.or(t._2.orNull()))
                .mapToPair(t -> new Tuple2<>(t._2, true))
                .persist(StorageLevel.MEMORY_AND_DISK_SER_2());

        // Partition into batches of at most partitionSize docs
        long docCount = allDocsByTable.count();

        return allDocsByTable
                .sortByKey(true, (int) Math.ceil(((float) docCount) / partitionSize))
                .foreachPartitionAsync(iter -> writeStashDocuments(iter, sqlContext, priorStash, newStash, databusSource));
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

    private JavaRDD<String> getDatabusEventsTablesRDD(SQLContext sqlContext, String databusSource,
                                                      ZonedDateTime priorStashTime, ZonedDateTime stashTime) {

        final Seq<Object> pollDates = getPollDates(priorStashTime, stashTime);
        final Dataset<Row> dataFrame = sqlContext.read().schema(DocumentSchema.SCHEMA).parquet(databusSource);

        return dataFrame.select(dataFrame.col(DocumentSchema.TABLE))
                .where(dataFrame.col(DocumentSchema.POLL_DATE).isin(pollDates))
                .distinct()
                .map(value -> value.getString(0), Encoders.STRING())
                .toJavaRDD();
    }

    private JavaPairRDD<DocumentId, DocumentLocation> getDocumentsWithDatabusUpdate(
            SQLContext sqlContext, String databusSource, ZonedDateTime priorStashTime, ZonedDateTime stashTime) {
        final Seq<Object> pollDates = getPollDates(priorStashTime, stashTime);
        final Dataset<Row> dataFrame = sqlContext.read().schema(DocumentSchema.SCHEMA).parquet(databusSource);

        return dataFrame
                .select(dataFrame.col(DocumentSchema.POLL_DATE), dataFrame.col(DocumentSchema.TABLE), dataFrame.col(DocumentSchema.KEY))
                .distinct()
                .where(dataFrame.col(DocumentSchema.POLL_DATE).isin(pollDates))
                .toJavaRDD()
                .mapToPair(row -> {
                    String pollDate = row.getString(0);
                    String table = row.getString(1);
                    String key = row.getString(2);
                    return new Tuple2<>(new DocumentId(table, key), DocumentLocation.parquet(table, pollDate, key));
                });
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

    private static void writeStashDocuments(Iterator<Tuple2<DocumentLocation, Boolean>> partitionDocs, SQLContext sqlContext,
                                     StashReader stashReader, StashWriter stashWriter, String databusSource) {
        PeekingIterator<DocumentLocation> docs = Iterators.peekingIterator(Iterators.transform(partitionDocs, Tuple2::_1));

        Tuple2<String, PeekingIterator<String>> tablePartition = readPartitionJsonForNextTable(docs, sqlContext, stashReader, databusSource);
        while (tablePartition != null) {
            String table = tablePartition._1;
            Iterator<String> jsonLines = tablePartition._2;

            // Use the hash from the first document to uniquely name the file
            String suffix = Hashing.md5().hashString(jsonLines.next()).toString();
            stashWriter.writeStashTableFile(table, suffix, jsonLines);
        }
    }

    private static Tuple2<String, PeekingIterator<String>> readPartitionJsonForNextTable(PeekingIterator<DocumentLocation> documents,
                                                           SQLContext sqlContext, StashReader stashReader, String databusSource) {
        while (documents.hasNext()) {
            final String table = documents.peek().getTableName();

            Iterator<String> jsonLines = new AbstractIterator<String>() {
                private Iterator<String> sourceDocs = Iterators.emptyIterator();

                @Override
                protected String computeNext() {
                    while (!sourceDocs.hasNext()) {
                        if (!documents.hasNext() || !documents.peek().getTableName().equals(table)) {
                            return endOfData();
                        }

                        if (documents.peek().inParquet()) {
                            sourceDocs = readPartitionJsonFromParquet(documents, sqlContext, databusSource);
                        } else {
                            sourceDocs = readPartitionJsonFromStashFile(documents, stashReader);
                        }
                    }
                    return sourceDocs.next();
                }
            };

            if (jsonLines.hasNext()) {
                return new Tuple2<>(table, Iterators.peekingIterator(jsonLines));
            }
        }

        return null;
    }

    private static Iterator<String> readPartitionJsonFromStashFile(PeekingIterator<DocumentLocation> documents, StashReader stashReader) {
        if (!documents.hasNext()) {
            return Iterators.emptyIterator();
        }

        final DocumentLocation initialLocation = documents.peek();

        final Iterator<Tuple2<Integer, String>> stashFileJson = stashReader.readStashTableFileJson(initialLocation.getTableName(), initialLocation.getStashFile());

        return new AbstractIterator<String>() {
            @Override
            protected String computeNext() {
                if (!documents.hasNext() || !documents.peek().fromSameSource(initialLocation)) {
                    return endOfData();
                }
                DocumentLocation documentLocation = documents.next();
                int nextLineNum = documentLocation.getStashLine();

                Tuple2<Integer, String> stashLine = null;
                while (stashFileJson.hasNext() && (stashLine = stashFileJson.next())._1 < nextLineNum) {
                    stashFileJson.next();
                }

                return stashLine._2();
            }
        };
    }

    private static Iterator<String> readPartitionJsonFromParquet(PeekingIterator<DocumentLocation> documents, SQLContext sqlContext, String databusSource) {
        if (!documents.hasNext()) {
            return Iterators.emptyIterator();
        }

        final DocumentLocation initialLocation = documents.peek();

        Builder<Object, Seq<Object>> keys = Seq$.MODULE$.newBuilder();
        while (documents.hasNext() && documents.peek().fromSameSource(initialLocation)) {
            keys.$plus$eq(documents.next().getParquetKey());
        }
        Seq<Object> keySeq = keys.result();

        // TODO:  THIS WON'T WORK
        final Dataset<Row> dataFrame = sqlContext.read().schema(DocumentSchema.SCHEMA).parquet(databusSource);

        return dataFrame.select(
                DocumentSchema.KEY, DocumentSchema.VERSION, DocumentSchema.DELETED, DocumentSchema.JSON)
                .where(
                        dataFrame.col(DocumentSchema.POLL_DATE).equalTo(initialLocation.getParquetPollDate())
                                .and(dataFrame.col(DocumentSchema.TABLE).equalTo(initialLocation.getTableName()))
                                .and(dataFrame.col(DocumentSchema.KEY).isin(keySeq)))
                .groupByKey(row -> row.getString(0), Encoders.STRING())
                .reduceGroups((left, right) -> left.getLong(1) > right.getLong(1) ? left : right)
                .toJavaRDD()
                .filter(t -> !t._2.getBoolean(2))
                .map(t -> t._2.getString(3))
                .toLocalIterator();
    }
}
