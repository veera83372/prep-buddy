package org.apache.prepbuddy.typesystem;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class TypeAnalyzerTest {
    @Test
    public void shouldBeAbleToGiveTheTypeStringOfGivenSample() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("facebook", "mpire", "teachstreet", "twitter"));
        assertEquals(DataType.ALPHANUMERIC_STRING, typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTheTypeAsNumericOfGivenSample() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList(".56", "2.56", "23", "2345676543245678.7654564", "45.34"));
        assertEquals(DataType.DECIMAL, typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTheTypeAsDecimalForDecimal() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("0.56", ".56", ".23", "2345676543245678.7654564"));
        assertEquals(DataType.DECIMAL, typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTheTypeAsInteger() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("56", "56", "23", "2345676543245678"));
        assertEquals(DataType.INTEGER, typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTheTypeAsEMAIL() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("max@fireworks.in", "jst@mls.co.in", "bil_man@cil.com", "bill.se@gmail.com"));
        assertEquals(DataType.EMAIL, typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTheTypeAsURL() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("http://some.com/file/xl", "http://www.si.com", "https://www.si.co.in"));
        assertEquals(DataType.URL, typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTheTypeAsCurrency() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("$2", "€223.78", "₹23"));
        assertEquals(DataType.CURRENCY, typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTheTypeAsSocialSecurityNumber() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("409-52-2002", "876-66-4978", "123-46-4878"));
        assertEquals(DataType.SOCIAL_SECURITY_NUMBER, typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTheTypeAsIPAddress() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("19.55.12.34", "11.45.78.34", "10.4.22.246", "10.4.22.246"));
        assertEquals(DataType.IP_ADDRESS, typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTheTypeAsZIPCode() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("12345", "23456-1234", "12345", "34567"));
        assertEquals(DataType.ZIP_CODE_US, typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTheTypeAsCountryCode2CHARACTER() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("IN","IR"));
        assertEquals(DataType.COUNTRY_CODE_2_CHARACTER, typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTypeAsCountryCode3CHARACTER() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("IND", "USA", "CHN"));
        assertEquals(DataType.COUNTRY_CODE_3_CHARACTER, typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTypeAsCountryName() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("India", "China", "South Africa", "United States", "Uganda"));
        assertEquals(DataType.COUNTRY_NAME, typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGIveTypeAsPhoneNumber() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("6723459812", "9992345678", "4576893245", "02345678901", "03465789012"));
        assertEquals(DataType.MOBILE_NUMBER, typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTypeAsTimeStamp() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("2010-07-21T08:52:05.222", "2016-05-20T12:51:00.282Z", "2016-05-20T13:04:41.632Z"));
        assertEquals(DataType.TIMESTAMP, typeAnalyzer.getType());
    }
}