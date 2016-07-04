package org.apache.datacommons.prepbuddy.utils

import org.apache.datacommons.prepbuddy.SparkTestCase
import org.junit.Assert._

class PivotTableTest extends SparkTestCase{
    test("pivotByCount should give pivoted table of given row and column indexes") {
        val pivotTable: PivotTable[Int] = new PivotTable[Int](0)
        pivotTable.addEntry("row", "column1", 5)

        val expected: Int = 5
        val actual: Int = pivotTable.valueAt("row", "column1")
        assertEquals(expected, actual)

        assert(0 == pivotTable.valueAt("row", "column"))

    }

    test("pivotTable can be transformed") {
        val pivotTable: PivotTable[Int] = new PivotTable[Int](0)
        pivotTable.addEntry("row", "column1", 5)
        pivotTable.addEntry("row", "column2", 6)


        val transformed: PivotTable[Any] = pivotTable.transform((value: Any) => {
            0.9
        }, 10)


        val value: Any = transformed.valueAt("row", "column1")
        assert(0.9 == value)

        assert(10 == transformed.valueAt("row", "column"))
    }
}
