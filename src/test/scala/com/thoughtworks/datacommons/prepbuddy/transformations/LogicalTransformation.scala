package com.thoughtworks.datacommons.prepbuddy.transformations
import com.thoughtworks.datacommons.prepbuddy.utils.RowRecord

import scala.util.control.Exception._

class LogicalTransformation extends GenericTransformation{

    def apply(rowRecord: RowRecord): Any = {
        val firstColumn: String = rowRecord(0)
        val headOption: Option[Boolean] = allCatch.opt(firstColumn.toBoolean)
        if (headOption.isDefined) {
            if (firstColumn.toBoolean) rowRecord(1) else rowRecord(2)
        } else {
            ""
        }
    }
}
