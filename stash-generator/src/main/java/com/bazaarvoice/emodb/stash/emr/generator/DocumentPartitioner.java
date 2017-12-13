package com.bazaarvoice.emodb.stash.emr.generator;

import com.bazaarvoice.emodb.stash.emr.DocumentId;
import com.google.common.collect.Maps;
import org.apache.spark.Partitioner;

import java.io.Serializable;
import java.util.Map;

public class DocumentPartitioner extends Partitioner {

    public final static int DEFAULT_PARTITION_SIZE = 10000;

    private final Map<String, TablePartition> _tablePartitions;
    private final int _numPartitions;

    public DocumentPartitioner(Map<String, Long> docCountsByTable) {
        this(docCountsByTable, DEFAULT_PARTITION_SIZE);
    }

    public DocumentPartitioner(Map<String, Long> docCountsByTable, int targetPartitionSize) {
        int partitionOffset = 0;

        _tablePartitions = Maps.newHashMap();
        for (Map.Entry<String, Long> entry : docCountsByTable.entrySet()) {
            int numPartitions = (int) Math.ceil(entry.getValue().doubleValue() / targetPartitionSize);
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
        DocumentId documentId = (DocumentId) o; 
        TablePartition tablePartition = _tablePartitions.get(documentId.getTable());
        return tablePartition.partitionOffset + Math.abs(documentId.getKey().hashCode()) % tablePartition.numPartitions;
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
