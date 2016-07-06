package org.apache.datacommons.prepbuddy.qualityanalyzers

import org.apache.datacommons.prepbuddy.SparkTestCase


class TypeAnalyzerTest extends SparkTestCase {
    test("should be Able to Detect the type as a string") {
        val dataSet = List("facebook", "mpire", "teachstreet", "twitter")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(ALPHANUMERIC_STRING == typeAnalyzer.getType)
    }
    test("should be able to detect type as decimal") {
        val dataSet = List(".56", "290.56", "23", "2345676543245678.7654564", "405.34")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(DECIMAL == typeAnalyzer.getType)
    }

}
