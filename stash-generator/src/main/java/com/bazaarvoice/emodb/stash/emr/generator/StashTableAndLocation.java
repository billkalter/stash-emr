package com.bazaarvoice.emodb.stash.emr.generator;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

import java.io.Serializable;

public class StashTableAndLocation implements Serializable, Comparable<StashTableAndLocation>{

    private final String _table;
    private final String _file;
    private final int _line;

    public StashTableAndLocation(String table, String file, int line) {
        _table = table;
        _file = file;
        _line = line;
    }

    public String getTable() {
        return _table;
    }

    public String getFile() {
        return _file;
    }

    public int getLine() {
        return _line;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StashTableAndLocation)) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }

        StashTableAndLocation that = (StashTableAndLocation) o;

        return _table.equals(that._table) &&
                _file.equals(that._file) &&
                _line == that._line;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_table, _file, _line);
    }

    @Override
    public int compareTo(StashTableAndLocation o) {
        return ComparisonChain.start()
                .compare(_table, o._table)
                .compare(_file, o._file)
                .compare(_line, o._line)
                .result();
    }
}
