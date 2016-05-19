package org.apache.prepbuddy.inferer;

import org.apache.prepbuddy.typesystem.BaseDataType;
import org.apache.prepbuddy.typesystem.DataType;
import org.junit.Assert;
import org.junit.Test;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class TypeAnalyzerTest {
    @Test
    public void shouldBeAbleToGiveTheTypeStringOfGivenSample() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("facebook", "mpire", "teachstreet"));
        assertEquals(DataType.ALPHANUMERIC_STRING,typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTheTypeAsNumericOfGivenSample() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList(".56","2.56","23","2345676543245678.7654564","45.34"));
        assertEquals(DataType.DECIMAL,typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTheTypeAsDecimalForDecimal() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("0.56",".56",".23","2345676543245678.7654564"));
        assertEquals(DataType.DECIMAL,typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTheTypeAsInteger() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("56","56","23","2345676543245678"));
        assertEquals(DataType.INTEGER,typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTheTypeAsEMAIL() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("max@fireworks.in","jst@mls.co.in","bil_man@cil.com","bill.se@gmail.com"));
        assertEquals(DataType.EMAIL,typeAnalyzer.getType());
    }
    @Test
    public void shouldBeAbleToGiveTheTypeAsURL() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("http://some.com/file/xl","http://www.si.com","https://www.si.co.in"));
        assertEquals(DataType.URL,typeAnalyzer.getType());
    }
    @Test
    public void shouldBeAbleToGiveTheTypeAsCurrency() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("$2","€223.78","₹23"));
        assertEquals(DataType.CURRENCY,typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTheTypeAsSocialSecurityNumber() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("409-52-2002","876-66-4978","123-46-4878"));
        assertEquals(DataType.SOCIAL_SECURITY_NUMBER,typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTheTypeAsIPAddress() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("19.55.12.34","11.45.78.34","10.4.22.246","10.4.22.246"));
        assertEquals(DataType.IP_ADDRESS,typeAnalyzer.getType());
    }
    @Test
    public void shouldBeAbleToGiveTheTypeAsZIPCode() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("12345-2345","23456-1234"));
        assertEquals(DataType.ZIP_CODE,typeAnalyzer.getType());
    }
}