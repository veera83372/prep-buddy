package org.apache.datacommons.prepbuddy.imputations

import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.datacommons.prepbuddy.utils.RowRecord
import org.apache.spark.rdd.RDD

class UnivariateLinearRegressionSubstitution(independentColumn: Int) extends strategy {
    private var slope: Double = 0
    private var intercept: Double = 0
    def setSlope(sumOfXs: Double, sumOfYs: Double, sumOfXYs: Double, sumOfSquared: Double, count: Long): Unit = {
        val nominator: Double = (count * sumOfXYs) - (sumOfXs * sumOfYs)
        val denominator: Double = (count * sumOfSquared) - sumOfXs * sumOfXs
        slope = nominator / denominator
    }

    def setIntercept(sumOfXs: Double, sumOfYs: Double, count: Long): Unit = {
        intercept = (sumOfYs - (slope * sumOfXs)) / count
    }

    override def prepareSubstitute(tRdd: TransformableRDD, missingDataColumn: Int): Unit = {
        val rdd: TransformableRDD = tRdd.removeRows((row) => {
            row.valueAt(missingDataColumn).trim.isEmpty || row.valueAt(independentColumn).trim.isEmpty
        })
        val xyRDD: RDD[Double] = rdd.multiplyColumns(missingDataColumn, independentColumn)
        val xSquareRDD: RDD[Double] = rdd.multiplyColumns(independentColumn, independentColumn)
        val yRDD: RDD[Double] = rdd.toDoubleRDD(missingDataColumn)
        val xRDD: RDD[Double] = rdd.toDoubleRDD(independentColumn)

        val sumOfXY: Double = xyRDD.sum()
        val sumOfSquareRDD: Double = xSquareRDD.sum()
        val sumOfX: Double = xRDD.sum()
        val sumOfY: Double = yRDD.sum()

        val count: Long = xSquareRDD.count()
        setSlope(sumOfX, sumOfY, sumOfXY, sumOfSquareRDD, count)
        setIntercept(sumOfX, sumOfY, count)
    }

    override def handleMissingData(record: RowRecord): String = {
        val independentValue: String = record.valueAt(independentColumn)
        try {
            val value: Double = independentValue.toDouble
            val imputedValue: Double = intercept + slope * value
            "%1.2f".format(imputedValue)
        }
        catch {
            case exp: NumberFormatException => ""
        }
    }
}
