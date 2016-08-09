package com.thoughtworks.datacommons.prepbuddy.transformations

import com.thoughtworks.datacommons.prepbuddy.utils.RowRecord

trait GenericTransformation {
    def apply(rowRecord: RowRecord): Any
}

trait IntegerTransformation extends GenericTransformation {
    def apply(rowRecord: RowRecord): Int

}

trait DecimalTransformation extends GenericTransformation {
    def apply(rowRecord: RowRecord): Double

}


