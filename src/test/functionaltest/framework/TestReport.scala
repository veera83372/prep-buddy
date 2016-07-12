package framework

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

class TestReport {
    private val results: ListBuffer[DatasetTestResults] = mutable.ListBuffer.empty

    def add(testResults: DatasetTestResults): Unit = results += testResults

    def show(): Unit = results.foreach(result => println("result = " + result.getOutcome))
}
