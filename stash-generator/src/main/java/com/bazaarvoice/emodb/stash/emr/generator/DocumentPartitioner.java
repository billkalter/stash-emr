package com.bazaarvoice.emodb.stash.emr.generator;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Maps;
import org.apache.spark.HashPartitioner;
import org.apache.spark.Partitioner;

import java.io.Serializable;
import java.util.Map;

public class DocumentPartitioner extends Partitioner {

    private final static long TARGET_DOCS_PER_PARTITION = 10000;

    private final Map<String, TablePartition> _tablePartitions;
    private final int _numPartitions;

    public DocumentPartitioner(Map<String, Long> docCountsByTable) {
        Map<String, Long> sortedCounts = ImmutableSortedMap.copyOf(docCountsByTable);
        int partitionOffset = 0;

        _tablePartitions = Maps.newHashMap();
        for (Map.Entry<String, Long> entry : sortedCounts.entrySet()) {
            int numPartitions = (int) Math.ceil(entry.getValue().doubleValue() / TARGET_DOCS_PER_PARTITION);
            _tablePartitions.put(entry.getKey(), new TablePartition(partitionOffset, numPartitions));
            partitionOffset += numPartitions;
        }

        _numPartitions = partitionOffset;
    }

    @Override
    public int numPartitions() {
        return _numPartitions;
    }

    @Override
    public int getPartition(Object o) {
        String key = (String) o;
        TablePartition tablePartition = _tablePartitions.get(key);
        return tablePartition.partitionOffset + Math.abs(key.hashCode()) % tablePartition.numPartitions;
    }

    private static final class TablePartition implements Serializable {
        int partitionOffset;
        int numPartitions;

        TablePartition(int partitionOffset, int numPartitions) {
            this.partitionOffset = partitionOffset;
            this.numPartitions = numPartitions;
        }
    }
}
