package com.bazaarvoice.emodb.stash.emr.sql;

import com.bazaarvoice.emodb.stash.emr.ContentEncoding;
import com.bazaarvoice.emodb.stash.emr.DocumentMetadata;
import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.UUID;

/**
 * Schema definition for Parquet documents.
 */
public class DocumentSchema {
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormatter.ofPattern("yyyyMMdd").withZone(ZoneOffset.UTC);

    public static final String UPDATE_ID = "updateId";
    public static final String TABLE = "table";
    public static final String KEY = "key";
    public static final String VERSION = "version";
    public static final String LAST_UPDATE_TS = "lastUpdateTs";
    public static final String DELETED = "deleted";
    public static final String ENCODING = "encoding";
    public static final String CONTENT = "content";
    public static final String POLL_DATE = "pollDate";

    public static final StructType SCHEMA = DataTypes.createStructType(
            ImmutableList.<StructField>builder()
                    .add(DataTypes.createStructField(UPDATE_ID, DataTypes.StringType, false))
                    .add(DataTypes.createStructField(TABLE, DataTypes.StringType, false))
                    .add(DataTypes.createStructField(KEY, DataTypes.StringType, false))
                    .add(DataTypes.createStructField(VERSION, DataTypes.LongType, false))
                    .add(DataTypes.createStructField(LAST_UPDATE_TS, DataTypes.LongType, false))
                    .add(DataTypes.createStructField(DELETED, DataTypes.BooleanType, false))
                    .add(DataTypes.createStructField(ENCODING, DataTypes.IntegerType, false))
                    .add(DataTypes.createStructField(CONTENT, DataTypes.BinaryType, true))
                    .add(DataTypes.createStructField(POLL_DATE, DataTypes.StringType, false))
                    .build());

    public static Row toRow(UUID id, DocumentMetadata metadata, ContentEncoding encoding, String json, ZonedDateTime pollTime) {
        return new GenericRow(new Object[] {
                id.toString(),
                metadata.getDocumentId().getTable(),
                metadata.getDocumentId().getKey(),
                metadata.getDocumentVersion().getVersion(),
                metadata.getDocumentVersion().getLastUpdateTs(),
                metadata.isDeleted(),
                encoding.getCode(),
                metadata.isDeleted() ? null : encoding.getEncoder().fromJson(json),
                toPollTime(pollTime)
        });
    }

    public static String toPollTime(ZonedDateTime pollTime) {
        return DATE_FORMAT.format(pollTime);
    }

    public static UUID toUpdateId(String updateId) {
        return UUID.fromString(updateId);
    }
}
