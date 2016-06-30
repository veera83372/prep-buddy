package org.apache.datacommons.prepbuddy.rdds

import org.apache.datacommons.prepbuddy.types.{CSV, FileType}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

import scala.collection.mutable


class TransformableRDD(parent: RDD[String], fileType: FileType = CSV) extends RDD[String](parent) {

    def dropColumn(columnIndex: Int): TransformableRDD = {
        val transformed: RDD[String] = this.map((record: String) => {
            val recordInBuffer: mutable.Buffer[String] = fileType.parseRecord(record).toBuffer
            recordInBuffer.remove(columnIndex)
            fileType.join(recordInBuffer.toArray)
        })
        new TransformableRDD(transformed, fileType)
    }


    override def filter(f: (String) => Boolean): TransformableRDD = new TransformableRDD(super.filter(f), fileType)

    @DeveloperApi
    override def compute(split: Partition, context: TaskContext): Iterator[String] = {
        parent.compute(split, context)
    }

    override protected def getPartitions: Array[Partition] = parent.partitions
}

