package com.bazaarvoice.emodb.stash.emr.generator;

import com.google.common.base.Objects;

import java.io.Serializable;

/**
 * POJO for Spark job to serve as an identifier for a unique EmoDB document in Stash.
 */
public class StashDocument implements Serializable {
    private final int _tableIndex;
    private final String _key;

    public StashDocument(int tableIndex, String key) {
        _tableIndex = tableIndex;
        _key = key;
    }

    public int getTableIndex() {
        return _tableIndex;
    }

    public String getKey() {
        return _key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof StashDocument)) {
            return false;
        }

        StashDocument that = (StashDocument) o;

        return _tableIndex == that._tableIndex && _key.equals(that._key);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_tableIndex, _key);
    }
}
