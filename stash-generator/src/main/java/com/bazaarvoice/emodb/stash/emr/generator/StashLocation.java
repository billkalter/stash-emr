package com.bazaarvoice.emodb.stash.emr.generator;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

import java.io.Serializable;

/**
 * POJO for Spark job to serve as an index to a unique JSON document from a prior Stash.  If the Spark job were to store
 * the JSON documents from a prior Stash in RDDs then memory pressure would quickly become a problem.  So instead of storing
 * the JSON itself in the RDD the location of the document is stored in the RDD.  If it is determined that the document
 * is unmodified the location can be used to re-read the document from the prior Stash and copy to the new Stash.
 */
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

    /**
     * Sort by file then line number.  This optimizes the ability to efficiently read documents back from the previous
     * Stash when sorted by location.
     */
    @Override
    public int compareTo(StashLocation o) {
        return ComparisonChain.start()
                .compare(_fileIndex, o._fileIndex)
                .compare(_line, o._line)
                .result();
    }
}
