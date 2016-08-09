package com.thoughtworks.datacommons.prepbuddy.transformations

import com.thoughtworks.datacommons.prepbuddy.utils.RowRecord

import scala.util.control.Exception._

class LogicalTransformation {
    def IF(predicateIndex: Int, returnValueIndex: Int, elseValueIndex: Int) :GenericTransformation= {
        new GenericTransformation {
            override def apply(rowRecord: RowRecord): Any = {
                val firstColumn: String = rowRecord(predicateIndex)
                val headOption: Option[Boolean] = allCatch.opt(firstColumn.toBoolean)
                if (headOption.isDefined) {
                    if (firstColumn.toBoolean) rowRecord(returnValueIndex) else rowRecord(elseValueIndex)
                } else {
                    ""
                }
            }
        }
    }
}
