package specs

import org.apache.spark.{SparkConf, SparkContext}

class FunctionalTestRunner extends App {

    protected val sparkContext: SparkContext = {
        val sparkConf: SparkConf = new SparkConf().setAppName(getClass.getName)
        new SparkContext(sparkConf)
    }

    //    private def printResults() {
    //        val testReport: TestReport = new TestReport()
    //        specs.foreach(spec => testReport.add(spec.getTestResults))
    //        testReport.show()
    //    }

    def shutDown(): Unit = sparkContext.stop()

    def test(testName: String)(testFunction: => Unit) {
        val exceptionalTestFun: () => Any = testFunction _
        try {
            exceptionalTestFun()
            println("PASSED => ".concat(testName))
        } catch {
            case _: Throwable => println("FAILED => ".concat(testName))
        }
    }
}