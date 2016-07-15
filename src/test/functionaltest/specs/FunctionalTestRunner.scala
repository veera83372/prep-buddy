package specs

import framework.{TestReport, TestResult}
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest

import scala.collection.mutable.ListBuffer

class FunctionalTestRunner extends App {
    private var testNames: ListBuffer[String] = ListBuffer.empty
    private val sparkConf: SparkConf = new SparkConf().setAppName(getClass.getName)

    private val testReport = new TestReport

    protected var sc: SparkContext = new SparkContext(sparkConf)

    def shutDown(): Unit = sc.stop()

    def test(testName: String)(testFunction: => Unit) {
        validateTestEnvironment(testName)
        val testResult: TestResult = runTest(testName, testFunction)
        testReport.add(testResult)
        shutDown()
    }

    def runTest(testName: String, testFunction: => Unit): TestResult = {
        val exceptionalTestFun: () => Any = testFunction _
        val testResult: TestResult = new TestResult(testName)
        try {
            exceptionalTestFun()
            testResult.markAsSuccess()
        } catch {
            case err: AssertionError => testResult.markAsFailure(err)
        }
        testResult
    }

    def validateTestEnvironment(testName: String) {
        if (testNames.contains(testName)) {
            throw new scalatest.DuplicateTestNameException(testName, -1)
        }
        if (sc.isStopped) {
            sc = new SparkContext(sparkConf)
        }
    }

    def printReport(): Unit = testReport.show()
}