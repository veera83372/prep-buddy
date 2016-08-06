package com.thoughtworks.datacommons.prepbuddy.normalizers

import com.thoughtworks.datacommons.prepbuddy.rdds.TransformableRDD

trait NormalizationStrategy extends Serializable {
    def prepare(transformableRDD: TransformableRDD, columnIndex: Int): Unit
    
    def normalize(rawValue: String): String
}
