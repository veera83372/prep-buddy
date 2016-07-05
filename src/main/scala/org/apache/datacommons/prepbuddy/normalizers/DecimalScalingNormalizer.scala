package org.apache.datacommons.prepbuddy.normalizers

import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.spark.rdd.RDD

class DecimalScalingNormalizer extends NormalizationStrategy {

    private var length = 0

    override def prepare(transformableRDD: TransformableRDD, columnIndex: Int): Unit = {
        val doubleRDD: RDD[Double] = transformableRDD.toDoubleRDD(columnIndex)
        length = String.valueOf(doubleRDD.max().intValue()).length()
    }

    override def normalize(rawValue: String): String = {
        String.valueOf(rawValue.toDouble / Math.pow(10, length - 1))
    }
}
