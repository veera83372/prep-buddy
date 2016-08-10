package com.thoughtworks.datacommons.prepbuddy.analyzers

import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

class RowCompletenessRule(possibleNullValues: List[String] = Nil) {
    private val nullValues = possibleNullValues :+ ""
    private var incompleteWhenAnyColumnIsNull = false
    private var specifiedColumns: List[String] = _
    
    def incompleteWhenAnyNull(): Unit = incompleteWhenAnyColumnIsNull = true
    
    def incompleteWhenNullAt(column: String, columns: String*): Unit = specifiedColumns = column :: columns.toList
    
    private def isNull(value: Any): Boolean = {
        value == null || nullValues.contains(value)
    }
    
    private def getColumnIndexesToConsider(schema: StructType): List[Int] = {
        if (incompleteWhenAnyColumnIsNull) {
            schema.fieldNames.indices.toList
        } else {
            specifiedColumns.map(schema.fieldNames.indexOf(_))
        }
    }
    
    def isComplete(record: Row): Boolean = {
        val columnIndexes: List[Int] = getColumnIndexesToConsider(record.schema)
        !columnIndexes
            .map(record.get)
            .foldLeft(false)((acc, current) => acc || isNull(current))
    }
}
