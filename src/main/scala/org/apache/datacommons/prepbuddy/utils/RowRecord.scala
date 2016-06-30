package org.apache.datacommons.prepbuddy.utils

class RowRecord(columnValues: Array[String]) {

  def valueAt(columnIndex: Int): String = {
     columnValues(columnIndex)
  }

  def length: Int = {
    columnValues.length
  }

  def hasEmptyColumn: Boolean = {
    columnValues.exists(_.trim.isEmpty)
//    for (columnValue <- columnValues) {
//      if (columnValue.trim.isEmpty) true
//    }
//    false
  }
}
