package org.apache.prepbuddy.transformations;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class ColumnMergerTest {
    @Test
    public void shouldMergeTheGivenColumnsWithTheGivenSeparatorByReplacingTheCurrentValues() {
        String[] inputRecord = "FirstName,LastName,732,MiddleName,XYZ".split(",");

        ColumnMerger columnMerger = new ColumnMerger(Arrays.asList(3, 1, 0), false, "_");
        String[] actualValue = columnMerger.apply(inputRecord);

        assertEquals("732,XYZ,MiddleName_LastName_FirstName", StringUtils.join(actualValue, ","));
    }


    @Test
    public void shouldMergeTheGivenColumnsAndPlaceTheResultAtTheEndOfTheRowByKeepingTheOriginalValue() {
        String[] inputRecord = "FirstName,LastName,732,MiddleName,One,Two,Three".split(",");

        ColumnMerger columnMerger = new ColumnMerger(Arrays.asList(4, 6, 1), true, " ");
        String[] actualValue = columnMerger.apply(inputRecord);

        assertEquals("FirstName,LastName,732,MiddleName,One,Two,Three,One Three LastName", StringUtils.join(actualValue, ","));
    }
}