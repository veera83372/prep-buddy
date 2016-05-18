package org.apache.prepbuddy.typesystem;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class TypeAnalyzerTest {
    @Test
    public void shouldInferAsAlphanumeric() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("facebook", "mpire", "teachstreet"));
        assertEquals(DataType.ALPHANUMERIC_STRING, typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTheTypeAsNumericOfGivenSample() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList(".56", "2.56", "23", "2345676543245678.7654564", "45"));
        assertEquals(DataType.DECIMAL, typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTheTypeAsDecimalForDecimal() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList(".56", "0.56", ".23", "2345676543245678.7654564"));
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
}