package org.apache.datacommons.prepbuddy.qualityanalyzers

import org.scalatest.FunSuite


class TypeAnalyzerTest extends FunSuite {
    test("should be Able to Detect the type as a string") {
        val dataSet = List("facebook", "mpire", "teachstreet", "twitter")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(ALPHANUMERIC_STRING == typeAnalyzer.getType)
    }
    test("should be able to detect type as decimal") {
        val dataSet = List(".56", "290.56", ".23", "2345676543245678.7654564", "405.34")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(DECIMAL == typeAnalyzer.getType)
    }

    test("should Be Able To Give The Type As Decimal For Decimal") {
        val dataSet = List("0.56", ".56", ".23", "2345676543245678.7654564")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(DECIMAL == typeAnalyzer.getType)
    }

}
