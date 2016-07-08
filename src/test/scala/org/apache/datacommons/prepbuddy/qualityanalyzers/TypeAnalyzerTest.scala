package org.apache.datacommons.prepbuddy.qualityanalyzers

import org.scalatest.FunSuite


class TypeAnalyzerTest extends FunSuite {
    test("should be Able to Detect the type as a string") {
        val dataSet = List("facebook", "Vampire", "teachStreet", "twitter")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(ALPHANUMERIC_STRING == typeAnalyzer.getType)
    }
    test("should be able to detect type as decimal") {
        val dataSet = List(".56", "290.56", "23", "2345676543245.7654564", "405.34", "234.65", "34.12")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(DECIMAL == typeAnalyzer.getType)
    }

    test("should Be Able To Give The Type As Decimal For Decimal") {
        val dataSet = List("12", "23", "0.56", ".56", ".23", "0.7654564", "67.44", "34.23", "12.12", "-0.23", "-23.0")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(DECIMAL == typeAnalyzer.getType)
    }
    test("should be able to give type as integer") {
        val dataSet = List("12", "23", "34", "-14", "-23")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(INTEGER == typeAnalyzer.getType)
    }
    test("should be able to give the type as email") {
        val dataSet = List("max@fireworks.in", "jst@mls.co.in", "bil_man@cil.com", "bill.se@gmail.com", "a@b.ci")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(EMAIL == typeAnalyzer.getType)
    }
}
