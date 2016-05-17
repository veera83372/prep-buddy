package org.apache.prepbuddy.inferer;

import org.apache.prepbuddy.filetypes.Type;
import org.junit.Test;
import java.util.Arrays;

import static org.junit.Assert.*;

public class TypeAnalyzerTest {
    @Test
    public void shouldBeAbleToGiveTheTypeStringOfGivenSample() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("facebook", "mpire", "teachstreet"));
        assertEquals(Type.STRING,typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTheTypeAsNumericOfGivenSample() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList(".56","0.56","23","2345676543245678.7654564","45"));
        assertEquals(Type.NUMERIC,typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTheTypeAsDecimalForDecimal() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList(".56","0.56",".23","2345676543245678.7654564"));
        assertEquals(Type.DECIMAL,typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTheTypeAsInteger() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("56","56","23","2345676543245678"));
        assertEquals(Type.INT,typeAnalyzer.getType());
    }

    @Test
    public void shouldBeAbleToGiveTheTypeAsEMAIL() {
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(Arrays.asList("max@fireworks.in","jst@mls.co.in","bil_man@cil.com","bill.se@gmail.com"));
        assertEquals(Type.EMAIL,typeAnalyzer.getType());
    }
}