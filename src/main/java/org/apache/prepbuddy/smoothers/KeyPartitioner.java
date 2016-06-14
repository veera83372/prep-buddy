package org.apache.prepbuddy.smoothers;

import org.apache.spark.Partitioner;

/**
 * A Patitioner that puts each row in a partition that we specify by key.
 */
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
