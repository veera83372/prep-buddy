package org.apache.prepbuddy.preprocessor;

import org.apache.prepbuddy.transformations.SparkTestCase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class RecordTrimmerTest extends SparkTestCase{
    
    @Test
    public void shouldTrimBothEndOfEachColumnForTheGivenRecords() {
        String result = new RecordTrimmer(",")
                                    .apply("     07784425582,     07981267897     ,Incoming,474,   Thu Sep 09 18:44:34 +0100 2010   ");
        assertEquals("07784425582,07981267897,Incoming,474,Thu Sep 09 18:44:34 +0100 2010", result);
    }

    @Test
    public void shouldTrimBothEndOfTheRecordWhenThereIsOnlyOneColumnInTheRecord() {
        String result = new RecordTrimmer(",")
                .apply("     076071 24303    ");
        assertEquals("076071 24303", result);

    }
}