package org.apache.prepbuddy.transformation;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ColumnSplitterTest {
    @Test
    public void shouldSplitTheGivenColumnByRemovingTheGivenColumn() {
        String[] inputRecord = "FirstName LastName MiddleName,850".split(",");

        SplitByDelimiter columnSplitter = new SplitByDelimiter(" ", false);
        String[] actualValue = columnSplitter.apply(inputRecord, 0);

        assertEquals("FirstName,LastName,MiddleName,850", StringUtils.join(actualValue, ","));
    }

    @Test
    public void shouldSplitTheGivenColumnToGivenNumberOfPartitionByRemovingTheGivenColumn() {
        String[] inputRecord = "FirstName LastName MiddleName,850".split(",");

        SplitByDelimiter columnSplitter = new SplitByDelimiter(" ", false, 2);
        String[] actualValue = columnSplitter.apply(inputRecord, 0);

        assertEquals("FirstName,LastName MiddleName,850", StringUtils.join(actualValue, ","));
    }

    @Test
    public void shouldSplitTheGivenColumnToGivenNumberOfPartitionByRetainingTheGivenColumn() {
        String[] inputRecord = "FirstName LastName MiddleName,850".split(",");

        SplitByDelimiter columnSplitter = new SplitByDelimiter(" ", true);
        String[] actualValue = columnSplitter.apply(inputRecord, 0);

        assertEquals("FirstName LastName MiddleName,FirstName,LastName,MiddleName,850", StringUtils.join(actualValue, ","));
    }
}