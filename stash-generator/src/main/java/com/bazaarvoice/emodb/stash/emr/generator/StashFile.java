package com.bazaarvoice.emodb.stash.emr.generator;

import com.google.common.base.Objects;

import java.io.Serializable;

/**
 * POJO for Spark job to serve as an identifier for a file from the prior Stash.
 */
public class StashFile implements Serializable {
    private final int _tableIndex;
    private final int _fileIndex;

    public StashFile(int tableIndex, int fileIndex) {
        _tableIndex = tableIndex;
        _fileIndex = fileIndex;
    }

    public int getTableIndex() {
        return _tableIndex;
    }

    public int getFileIndex() {
        return _fileIndex;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StashFile)) {
            return false;
        }

        StashFile stashFile = (StashFile) o;

        return _tableIndex == stashFile._tableIndex && _fileIndex == stashFile._fileIndex;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_tableIndex, _fileIndex);
    }
}
