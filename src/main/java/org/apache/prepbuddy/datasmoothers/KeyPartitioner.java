package org.apache.prepbuddy.datasmoothers;

import org.apache.spark.Partitioner;

public class KeyPartitioner extends Partitioner {
    private int numPartitions;

    public KeyPartitioner(int numPartitions) {
        this.numPartitions = numPartitions;
    }

    @Override
    public int numPartitions() {
        return numPartitions;
    }

    @Override
    public int getPartition(Object key) {
        return (int) key;
    }
}
