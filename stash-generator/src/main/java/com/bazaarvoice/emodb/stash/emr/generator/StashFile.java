package com.bazaarvoice.emodb.stash.emr.generator;

import com.google.common.base.Objects;

import java.io.Serializable;

public class StashFile implements Serializable {
    private final String _table;
    private final String _file;

    public StashFile(String table, String file) {
        _table = table;
        _file = file;
    }

    public String getTable() {
        return _table;
    }

    public String getFile() {
        return _file;
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

        return _table.equals(stashFile._table) && _file.equals(stashFile._file);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_table, _file);
    }
}
