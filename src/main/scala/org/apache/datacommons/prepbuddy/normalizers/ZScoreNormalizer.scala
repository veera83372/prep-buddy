package org.apache.datacommons.prepbuddy.normalizers

import org.apache.datacommons.prepbuddy.rdds.TransformableRDD

class ZScoreNormalizer extends NormalizationStrategy {

    private var standardDeviation: Double = 0

    private var mean: Double = 0

    override def prepare(transformableRDD: TransformableRDD, columnIndex: Int): Unit = {
        val doubleRDD = transformableRDD.toDoubleRDD(columnIndex)
        standardDeviation = doubleRDD.stdev
        mean = doubleRDD.mean
    }

    override def normalize(rawValue: String): String = {
        String.valueOf((rawValue.toDouble - mean) / standardDeviation)
    }
}
