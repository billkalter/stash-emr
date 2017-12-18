package com.bazaarvoice.emodb.stash.emr.generator;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

import java.io.Serializable;

public class StashLocation implements Serializable, Comparable<StashLocation> {
    private final String _file;
    private final int _line;

    public StashLocation(String file, int line) {
        _file = file;
        _line = line;
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
        if (!(o instanceof StashLocation)) {
            return false;
        }

        StashLocation that = (StashLocation) o;

        return _file.equals(that._file) && _line == that._line;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_file, _line);
    }

    @Override
    public int compareTo(StashLocation o) {
        return ComparisonChain.start()
                .compare(_file, o._file)
                .compare(_line, o._line)
                .result();
    }
}
