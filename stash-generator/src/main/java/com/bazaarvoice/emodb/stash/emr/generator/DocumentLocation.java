package com.bazaarvoice.emodb.stash.emr.generator;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.Ordering;

import java.io.Serializable;

public class DocumentLocation implements Serializable, Comparable<DocumentLocation> {

    private final String _tableName;
    private final String _stashFile;
    private final int _stashLine;
    private final String _parquetPollDate;
    private final String _parquetKey;

    public static DocumentLocation parquet(String tableName, String parquetPollDate, String parquetKey) {
        return new DocumentLocation(tableName, null, -1, parquetPollDate, parquetKey);
    }

    public static DocumentLocation stash(String tableName, String stashFile, int stashLine) {
        return new DocumentLocation(tableName, stashFile, stashLine, null, null);
    }

    private DocumentLocation(String tableName, String stashFile, int stashLine, String parquetPollDate, String parquetKey) {
        _tableName = tableName;
        _stashFile = stashFile;
        _stashLine = stashLine;
        _parquetPollDate = parquetPollDate;
        _parquetKey = parquetKey;
    }

    public String getTableName() {
        return _tableName;
    }

    public boolean inParquet() {
        return _stashFile == null;
    }

    public String getStashFile() {
        return _stashFile;
    }

    public int getStashLine() {
        return _stashLine;
    }

    public String getParquetPollDate() {
        return _parquetPollDate;
    }

    public String getParquetKey() {
        return _parquetKey;
    }

    @Override
    public int compareTo(DocumentLocation o) {
        return ComparisonChain.start()
                .compare(_tableName, o._tableName)
                .compare(_stashFile, o._stashFile, Ordering.natural().nullsLast())
                .compare(_stashLine, o._stashLine)
                .compare(_parquetPollDate, o._parquetPollDate)
                .compare(_parquetKey, o._parquetKey)
                .result();
    }

    public boolean fromSameSource(DocumentLocation o) {
        return _tableName.equals(o._tableName) &&
                Objects.equal(_stashFile, o._stashFile) &&
                Objects.equal(_parquetPollDate, o._parquetPollDate);
    }
}
