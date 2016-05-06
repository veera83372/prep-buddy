package org.apache.prepbuddy.transformation;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ColumnSplitterTest {
    @Test
    public void shouldSplitTheGivenColumn() {
        String[] inputRecord = "FirstName LastName MiddleName,850".split(",");

        ColumnSplitter columnSplitter = new ColumnSplitter(0, " ");
        String[] actualValue = columnSplitter.apply(inputRecord);

        assertEquals("FirstName,LastName,MiddleName,850", StringUtils.join(actualValue, ","));
    }

    @Test
    public void shouldSplitTheGivenColumnToGivenNumberOfPartition() {
        String[] inputRecord = "FirstName LastName MiddleName,850".split(",");

        ColumnSplitter columnSplitter = new ColumnSplitter(0, " ", 2);
        String[] actualValue = columnSplitter.apply(inputRecord);

        assertEquals("FirstName,LastName MiddleName,850", StringUtils.join(actualValue, ","));
    }
}