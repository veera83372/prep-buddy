package org.apache.prepbuddy.transformation;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ColumnSplitTest {
    @Test
    public void shouldSplitTheGivenColumn() {
        String[] inputRecord = "FirstName LastName MiddleName,850".split(",");

        ColumnSplit columnSplit = new ColumnSplit(0, " ");
        String[] actualValue = columnSplit.apply(inputRecord);

        assertEquals("FirstName,LastName,MiddleName,850", StringUtils.join(actualValue, ","));
    }
}