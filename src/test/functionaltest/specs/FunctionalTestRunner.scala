package specs

import framework.{DatasetTestSpec, TestReport, TestableDataset}
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class FunctionalTestRunner extends {
    private val specs: ListBuffer[DatasetTestSpec] = mutable.ListBuffer.empty

    protected val sparkContext: SparkContext = getSparkContext

    private def getSparkContext: SparkContext = {
        val sparkConf: SparkConf = new SparkConf().setAppName(getClass.getName)
        new SparkContext(sparkConf)
    }

    def addSpec(testSpec: DatasetTestSpec) {
        specs += testSpec
    }

    def run(): Unit = specs.foreach(_.execute(sparkContext))

    def printResults() {
        val testReport: TestReport = new TestReport()
        specs.foreach(spec => testReport.add(spec.getTestResults))
        testReport.show()
    }

    def shutDown(): Unit = sparkContext.stop()
}

object FunctionalTestRunner {
    def main(args: Array[String]) {
        val testRunner: FunctionalTestRunner = new FunctionalTestRunner
        val dataset: TestableDataset = new TestableDataset(args(0))
        val typeInferenceTestSpec: TypeInferenceTestSpec = new TypeInferenceTestSpec(dataset)

        testRunner.addSpec(typeInferenceTestSpec)

        testRunner.run()
        testRunner.printResults()
        testRunner.shutDown()
    }
}