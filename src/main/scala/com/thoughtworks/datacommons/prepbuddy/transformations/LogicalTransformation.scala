package com.thoughtworks.datacommons.prepbuddy.transformations

import com.thoughtworks.datacommons.prepbuddy.utils.RowRecord

import scala.util.control.Exception._

class LogicalTransformation {
    def AND(indexes: Int*): GenericTransformation = {
        new GenericTransformation {
            override def apply(rowRecord: RowRecord): Any = {
                indexes.foreach(item=> {
                    val headOption: Option[Boolean] = allCatch.opt(rowRecord(item).toBoolean)
                    if(headOption.isEmpty) return "null"
                    if(!rowRecord(item).toBoolean) return "false"
                })
                "true"
            }
        }
    }

    def IF(predicateIndex: Int, returnValueIndex: Int, elseValueIndex: Int): GenericTransformation = {
        new GenericTransformation {
            override def apply(rowRecord: RowRecord): Any = {
                val firstColumn: String = rowRecord(predicateIndex)
                val headOption: Option[Boolean] = allCatch.opt(firstColumn.toBoolean)
                if (headOption.isEmpty) return "null"
                if (firstColumn.toBoolean) rowRecord(returnValueIndex) else rowRecord(elseValueIndex)
            }
        }
    }
}
