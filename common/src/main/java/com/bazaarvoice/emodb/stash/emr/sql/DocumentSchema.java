package com.bazaarvoice.emodb.stash.emr.sql;

import com.bazaarvoice.emodb.stash.emr.DocumentId;
import com.bazaarvoice.emodb.stash.emr.DocumentMetadata;
import com.bazaarvoice.emodb.stash.emr.DocumentVersion;
import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.catalyst.expressions.GenericRow;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

public class DocumentSchema {
    private static final SimpleDateFormat DATE_FORMAT = new SimpleDateFormat("yyyyMMdd");
    static {
        DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
    }

    public static final String TABLE = "table";
    public static final String KEY = "key";
    public static final String VERSION = "version";
    public static final String LAST_UPDATE_TS = "lastUpdateTs";
    public static final String DELETED = "deleted";
    public static final String JSON = "json";
    public static final String POLL_DATE = "pollDate";

    public static final StructType SCHEMA = DataTypes.createStructType(
            ImmutableList.<StructField>builder()
                    .add(DataTypes.createStructField(TABLE, DataTypes.StringType, false))
                    .add(DataTypes.createStructField(KEY, DataTypes.StringType, false))
                    .add(DataTypes.createStructField(VERSION, DataTypes.LongType, false))
                    .add(DataTypes.createStructField(LAST_UPDATE_TS, DataTypes.LongType, false))
                    .add(DataTypes.createStructField(DELETED, DataTypes.BooleanType, false))
                    .add(DataTypes.createStructField(JSON, DataTypes.StringType, false))
                    .add(DataTypes.createStructField(POLL_DATE, DataTypes.StringType, false))
                    .build());

    public static Row toRow(DocumentMetadata metadata, String json, Date pollDate) {
        return new GenericRow(new Object[] {
                metadata.getDocumentId().getTable(),
                metadata.getDocumentId().getKey(),
                metadata.getDocumentVersion().getVersion(),
                metadata.getDocumentVersion().getLastUpdateTs(),
                metadata.isDeleted(),
                json,
                DATE_FORMAT.format(pollDate)

        });
    }

    public static String getTable(Row row) {
        return row.getString(0);
    }

    public static DocumentMetadata getMetadata(Row row) {
        return new DocumentMetadata(
                new DocumentId(row.getString(0), row.getString(1)),
                new DocumentVersion(row.getLong(2), row.getLong(3)),
                row.getBoolean(4));
    }

    public static String getJson(Row row) {
        return row.getString(5);
    }
}
