package framework

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

abstract class DatasetTestSpec(dataset: TestableDataset) {
    protected var testResults: DatasetTestResults

    def execute(sparkContext: SparkContext) {
        val testableRDD: RDD[String] = sparkContext.textFile(dataset.fileName)
        testResults = new DatasetTestResults(getClass.getCanonicalName)
        try {
            executeTest(testableRDD)
            testResults.markAsSuccess()
        } catch {
            case error: AssertionError => testResults.markAsFailure(error)
        }
    }

    def getTestResults: DatasetTestResults = testResults

    def executeTest(testableRDD: RDD[String]): Unit
}
