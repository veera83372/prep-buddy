package com.thoughtworks.datacommons.prepbuddy.analyzers.completeness

import com.thoughtworks.datacommons.prepbuddy.analyzers.completeness.EvaluationMode.EvaluationMode
import com.thoughtworks.datacommons.prepbuddy.exceptions.{ApplicationException, ErrorMessages}
import org.apache.spark.sql._
import org.apache.spark.sql.types.StructType

class RowCompletenessRule(evalMode: EvaluationMode,
                          mandatoryColumns: List[String] = Nil,
                          possibleNullValues: List[String] = Nil) extends Serializable {
    
    if (evalMode == EvaluationMode.CUSTOM && mandatoryColumns == Nil) {
        throw new ApplicationException(ErrorMessages.REQUIREMENT_NOT_MATCHED)
    }
    
    private val nullValues = possibleNullValues ::: List("", "\t")
    private var columnIndexesToConsider: List[Int] = _ //For optimization purpose
    
    def isComplete(record: Row): Boolean = {
        val columnIndexes: List[Int] = getColumnIndexesToConsider(record.schema)
        !columnIndexes
                .map(record.get)
                .foldLeft(false)((acc, current) => acc || isNull(current))
    }
    
    private def isNull(value: Any): Boolean = {
        value == null || nullValues.contains(value)
    }
    
    private def getColumnIndexesToConsider(schema: StructType): List[Int] = {
        if (columnIndexesToConsider != null) return columnIndexesToConsider
        
        columnIndexesToConsider = if (evalMode == EvaluationMode.STRICT) {
            schema.fieldNames.indices.toList
        } else {
            mandatoryColumns.map { colName =>
                if (schema.fieldNames.contains(colName)) {
                    schema.fieldNames.indexOf(colName)
                } else {
                    throw new ApplicationException(ErrorMessages.COLUMN_NOT_FOUND)
                }
            }
        }
        columnIndexesToConsider
    }
}
