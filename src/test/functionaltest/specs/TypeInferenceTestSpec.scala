package specs

import framework.{DatasetAssertion, DatasetTestResults, DatasetTestSpec, TestableDataset}
import org.apache.datacommons.prepbuddy.qualityanalyzers.{DataType, MOBILE_NUMBER}
import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.spark.rdd.RDD

class TypeInferenceTestSpec(dataset: TestableDataset) extends DatasetTestSpec(dataset) {

    override protected var testResults: DatasetTestResults = _

    override def executeTest(testableRDD: RDD[String]): Unit = {
        val transformableRDD: TransformableRDD = new TransformableRDD(testableRDD)
        transformableRDD.cache()
        val actualDatatype: DataType = transformableRDD.inferType(0)
        DatasetAssertion.assertType(MOBILE_NUMBER, actualDatatype)
    }
}
