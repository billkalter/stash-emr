package com.bazaarvoice.emodb.stash.emr.generator;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

import java.io.Serializable;

public class StashLocation implements Serializable, Comparable<StashLocation> {
    private final int _fileIndex;
    private final int _line;

    public StashLocation(int fileIndex, int line) {
        _fileIndex = fileIndex;
        _line = line;
    }

    public int getFileIndex() {
        return _fileIndex;
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

        return _fileIndex == that._fileIndex && _line == that._line;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_fileIndex, _line);
    }

    @Override
    public int compareTo(StashLocation o) {
        return ComparisonChain.start()
                .compare(_fileIndex, o._fileIndex)
                .compare(_line, o._line)
                .result();
    }
}
