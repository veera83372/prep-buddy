package org.apache.prepbuddy.transformers;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class SplitPlanTest {
    @Test
    public void shouldSplitTheGivenColumnValueAccordingToTheGivenLengths() {
        String[] inputRecord = "FirstName LastName MiddleName,850".split(",");

        SplitPlan splitPlan = new SplitPlan(0, Arrays.asList(9, 9), false);
        String[] actualValue = splitPlan.splitColumn(inputRecord);

        assertEquals("FirstName, LastName,850", StringUtils.join(actualValue, ","));
    }

    @Test
    public void shouldSplitTheGivenColumnByDelimiterWhileRemovingTheGivenColumn() {
        String[] inputRecord = "FirstName LastName MiddleName,850".split(",");

        SplitPlan splitByDelimiter = new SplitPlan(0, " ", false);
        String[] actualValue = splitByDelimiter.splitColumn(inputRecord);

        assertEquals("FirstName,LastName,MiddleName,850", StringUtils.join(actualValue, ","));
    }

    @Test
    public void shouldSplitTheGivenColumnByGivenDelimiterToGivenNumberOfColumnsByRemovingTheGivenColumn() {
        String[] inputRecord = "FirstName LastName MiddleName,850".split(",");

        SplitPlan splitByDelimiter = new SplitPlan(0, " ", 2, false);
        String[] actualValue = splitByDelimiter.splitColumn(inputRecord);

        assertEquals("FirstName,LastName MiddleName,850", StringUtils.join(actualValue, ","));
    }

    @Test
    public void shouldSplitTheGivenColumnByGivenDelimiterToGivenNumberOfPartitionByRetainingTheGivenColumn() {
        String[] inputRecord = "FirstName LastName MiddleName,850".split(",");

        SplitPlan splitByDelimiter = new SplitPlan(0, " ", true);
        String[] actualValue = splitByDelimiter.splitColumn(inputRecord);

        assertEquals("FirstName LastName MiddleName,FirstName,LastName,MiddleName,850", StringUtils.join(actualValue, ","));
    }
}