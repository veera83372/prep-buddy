package org.apache.datacommons.prepbuddy.qualityanalyzers

import org.scalatest.FunSuite


class TypeAnalyzerTest extends FunSuite {
    test("should be Able to Detect the type as a string") {
        val dataSet = List("facebook", "Vampire", "teachStreet", "twitter")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(ALPHANUMERIC_STRING equals typeAnalyzer.getType)
    }
    test("should be able to detect type as decimal") {
        val dataSet = List(".56", "290.56", "23", "2345676543245.7654564", "405.34", "234.65", "34.12")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(DECIMAL equals typeAnalyzer.getType)
    }

    test("should Be Able To Give The Type As Decimal For Decimal") {
        val dataSet = List("12", "23", "0.56", ".56", ".23", "0.7654564", "67.44", "34.23", "12.12", "-0.23", "-23.0")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(DECIMAL equals typeAnalyzer.getType)
    }
    test("should be able to give type as integer") {
        val dataSet = List("12", "23", "34", "-14", "-23")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(INTEGER eq typeAnalyzer.getType)
    }
    test("should be able to give the type as email") {
        val dataSet = List("max@fireworks.in", "jst@mls.co.in", "bil_man@cil.com", "bill.se@gmail.com", "a@b.ci")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(EMAIL equals typeAnalyzer.getType)
    }
    test("should be able to give the type as currency") {
        val dataSet: List[String] = List("$2", "€223.78", "₹23")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(CURRENCY equals typeAnalyzer.getType)
    }
    test("should give the type as String if found multiple type") {
        val dataSet: List[String] = List("$2", "€223.78", "₹23", "max@fireworks.in", "jst@mls.co.in")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(ALPHANUMERIC_STRING equals typeAnalyzer.getType)
    }
    test("should be Able to give the type as URL") {
        val dataSet: List[String] = List("http://some.com/file/xl", "http://www.si.com", "https://www.si.co.in")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(URL equals typeAnalyzer.getType)
    }
    test("should be able to give IP Address as Type") {
        val dataSet: List[String] = List("19.55.12.34", "11.45.78.34", "10.4.22.246", "10.4.22.246")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(IP_ADDRESS equals typeAnalyzer.getType)
    }
    test("should be able to give type as US ZIP Code") {
        val dataSet: List[String] = List("12345", "23456-1234", "12345", "34567")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(ZIP_CODE_US equals typeAnalyzer.getType)
    }
    test("should be able to give type as mobile Number") {
        val dataSet: List[String] = List("6723459812", "9992345678", "04576893245", "02345678901", "03465789012")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(MOBILE_NUMBER equals typeAnalyzer.getType)
    }

    test("should Be Able To Give Type As Mobile Number With Code") {
        val dataSet: List[String] = List("+12 6723459812", "+92 9992345678", "+343 2424432489", "+324 543545445")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(MOBILE_NUMBER equals typeAnalyzer.getType)
    }

    test("should be able to give type as timestamp") {
        val dataSet = List("2010-07-21T08:52:05.222", "2016-05-20T12:51:00.282Z", "2016-05-20T13:04:41.632Z")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(TIMESTAMP eq typeAnalyzer.getType)
    }

    test("should Be Able To Detect Longitude As Type") {
        val dataSet: List[String] = List("-122.42045568910925", "-73.9683", "-155.93665", "-107.4799714831678")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(LONGITUDE eq typeAnalyzer.getType)
    }

    test("should Be Able To Detect LATITUDE As Type") {
        val dataSet: List[String] = List("+40.2201", "+40.7415", "+19.05939", "+36.99409882257", "-82.180507982090035")
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(dataSet)
        assert(LATITUDE eq typeAnalyzer.getType)
    }
    test("should be able to give type as social security Number") {
        val typeAnalyzer: TypeAnalyzer = new TypeAnalyzer(List("409-52-2002", "876-66-4978", "123-46-4878"))
        assert(SOCIAL_SECURITY_NUMBER eq typeAnalyzer.getType)
    }
}
