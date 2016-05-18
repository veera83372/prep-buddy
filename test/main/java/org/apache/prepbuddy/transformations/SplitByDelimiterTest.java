package org.apache.prepbuddy.transformations;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class SplitByDelimiterTest {
    @Test
    public void shouldSplitTheGivenColumnByRemovingTheGivenColumn() {
        String[] inputRecord = "FirstName LastName MiddleName,850".split(",");

        SplitByDelimiter splitByDelimiter = new SplitByDelimiter(" ", false);
        String[] actualValue = splitByDelimiter.apply(inputRecord, 0);

        assertEquals("FirstName,LastName,MiddleName,850", StringUtils.join(actualValue, ","));
    }

    @Test
    public void shouldSplitTheGivenColumnToGivenNumberOfPartitionByRemovingTheGivenColumn() {
        String[] inputRecord = "FirstName LastName MiddleName,850".split(",");

        SplitByDelimiter splitByDelimiter = new SplitByDelimiter(" ",  false, 2 );
        String[] actualValue = splitByDelimiter.apply(inputRecord, 0);

        assertEquals("FirstName,LastName MiddleName,850", StringUtils.join(actualValue, ","));
    }

    @Test
    public void shouldSplitTheGivenColumnToGivenNumberOfPartitionByRetainingTheGivenColumn() {
        String[] inputRecord = "FirstName LastName MiddleName,850".split(",");

        SplitByDelimiter splitByDelimiter = new SplitByDelimiter(" ", true);
        String[] actualValue = splitByDelimiter.apply(inputRecord, 0);

        assertEquals("FirstName LastName MiddleName,FirstName,LastName,MiddleName,850", StringUtils.join(actualValue, ","));
    }
}