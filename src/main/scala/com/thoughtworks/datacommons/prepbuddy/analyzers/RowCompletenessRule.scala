package com.thoughtworks.datacommons.prepbuddy.analyzers

import org.apache.spark.sql._

class RowCompletenessRule(possibleNullValues: List[String] = Nil) {
    private val nullValues = possibleNullValues.map(_.toLowerCase)
    private var specifiedColumns: List[String] = _
    
    def incompleteWhenNullAt(column: String, columns: String*): Unit = specifiedColumns = column :: columns.toList
    
    private val incompleteWhenAnyColumnIsNull = true
    
    //    def incompleteWhenAnyNull: Unit = ???
    
    private def isNull(value: String): Boolean = {
        value == null || value.isEmpty || nullValues.contains(value.toLowerCase)
    }
    
    def isComplete(record: Row): Boolean = {
        !specifiedColumns
            .map(record.getAs[String])
            .foldLeft(false)((acc, current) => acc || isNull(current))
    }
}
