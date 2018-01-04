package com.bazaarvoice.emodb.stash.emr.generator;

import com.google.common.collect.ComparisonChain;

import java.io.Serializable;

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
    public int compareTo(TableAndValue<T> o) {
        return ComparisonChain.start()
                .compare(_tableIndex, o._tableIndex)
                .compare(_value, o._value)
                .result();
    }
}
