package framework

import org.apache.datacommons.prepbuddy.qualityanalyzers.DataType
import org.junit.Assert
import org.junit.Assert._

object DatasetAssertion extends Assert {
    def assertType(expected: DataType, actual: DataType) {
        assertEquals(expected, actual)
    }
}
