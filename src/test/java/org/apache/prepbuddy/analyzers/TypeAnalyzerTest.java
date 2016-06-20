package org.apache.prepbuddy.analyzers;

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
    public void shouldBeAbleToGiveTheTypeAsDecimalOfGivenSample() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList(".56", "290.56", "23", "2345676543245678.7654564", "405.34"));
        assertEquals(DataType.DECIMAL, typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTheTypeAsDecimalForDecimal() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("0.56", ".56", ".23", "2345676543245678.7654564"));
        assertEquals(DataType.DECIMAL, typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTheTypeAsInteger() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("569", "506", "23", "2345676543245678", "23456"));
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
    public void shouldBeAbleToGIveTypeAsMobileNumber() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("6723459812", "9992345678", "04576893245", "02345678901", "03465789012"));
        assertEquals(DataType.MOBILE_NUMBER, typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGIveTypeAsMobileNumberWithCode() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("-12 6723459812", "+92 9992345678", "+343 2424432489", "+324 543545445"));
        assertEquals(DataType.MOBILE_NUMBER, typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTypeAsTimeStamp() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("2010-07-21T08:52:05.222", "2016-05-20T12:51:00.282Z", "2016-05-20T13:04:41.632Z"));
        assertEquals(DataType.TIMESTAMP, typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToDetectLongitudeAsType() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("-122.42045568910925", "-73.9683", "-155.93665", "-107.4799714831678"));
        assertEquals(DataType.LONGITUDE, typeAnalyzer.getType());
    }


    @Test
    public void shouldBeAbleToDetectLatitudeAsType() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("40.2201", "40.7415", "19.05939", "36.994098822572425", "-82.180507982090035"));
        assertEquals(DataType.LATITUDE, typeAnalyzer.getType());
    }
}