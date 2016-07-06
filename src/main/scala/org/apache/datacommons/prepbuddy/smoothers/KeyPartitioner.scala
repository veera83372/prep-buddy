package org.apache.datacommons.prepbuddy.smoothers

import org.apache.spark.Partitioner

class KeyPartitioner(numOfPartitions: Int) extends Partitioner{
    override def numPartitions: Int = numOfPartitions

    override def getPartition(key: Any): Int = key.toString.toInt
}
