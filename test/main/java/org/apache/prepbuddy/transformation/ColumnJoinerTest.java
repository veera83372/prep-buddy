package org.apache.prepbuddy.transformation;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class ColumnJoinerTest {
    @Test
    public void shouldMargeTheGivenColumnsWithTheGivenSeperatorByReplacingTheCurrentValues() {
        String[] inputRecord = "FirstName,LastName,732,MiddleName".split(",");

        ColumnJoiner columnJoiner = new ColumnJoiner(Arrays.asList(3, 1, 0), "_", false);
        String[] actualValue = columnJoiner.apply(inputRecord);

        assertEquals("732,MiddleName_LastName_FirstName", StringUtils.join(actualValue, ","));
    }


    @Test
    public void shouldMargeTheGivenColumnsAndPlaceTheResultAtTheEndOfTheRowByKeepingTheOriginalValue() {
        String[] inputRecord = "FirstName,LastName,732,MiddleName,One,Two,Three".split(",");

        ColumnJoiner columnJoiner = new ColumnJoiner(Arrays.asList(4, 6, 1), " ", true);
        String[] actualValue = columnJoiner.apply(inputRecord);

        assertEquals("FirstName,LastName,732,MiddleName,One,Two,Three,One Three LastName", StringUtils.join(actualValue, ","));
    }
}