package com.bazaarvoice.emodb.stash.emr.generator;

import com.google.common.base.Objects;
import com.google.common.collect.ComparisonChain;

import java.io.Serializable;

/**
 * Several RDDs used by the Spark job create sorted partitions by sorting primarily by table and secondarily by some
 * other value.  This class provides a tuple which maintains this sort.
 */
public class TableAndValue<T extends Serializable & Comparable<T>> implements Serializable, Comparable<TableAndValue<T>> {
    
    private final int _tableIndex;
    private final T _value;

    public TableAndValue(int tableIndex, T value) {
        _tableIndex = tableIndex;
        _value = value;
    }

    public int getTableIndex() {
        return _tableIndex;
    }

    public T getValue() {
        return _value;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TableAndValue)) {
            return false;
        }

        TableAndValue<?> that = (TableAndValue<?>) o;

        return _tableIndex == that._tableIndex && Objects.equal(_value, that._value);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(_tableIndex, _value);
    }

    @Override
    public int compareTo(TableAndValue<T> o) {
        return ComparisonChain.start()
                .compare(_tableIndex, o._tableIndex)
                .compare(_value, o._value)
                .result();
    }
}
