package org.apache.prepbuddy.transformation;

import org.apache.commons.lang.StringUtils;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class ColumnJoinTest {
    @Test
    public void shouldMargeTheGivenColumnsWithTheGivenSeperator() {
        String[] inputRecord = "FirstName,LastName,732,MiddleName".split(",");

        ColumnJoin columnJoin = new ColumnJoin(Arrays.asList(3, 1, 0), "_");
        String[] actualValue = columnJoin.apply(inputRecord);

        assertEquals("732,MiddleName_LastName_FirstName", StringUtils.join(actualValue, ","));
    }

    @Test
    public void shouldMargeTheGivenColumnsWithSpaceWhenNoSeperatorIsProvided() {
        String[] inputRecord = "FirstName,LastName,732,MiddleName,One,Two,Three".split(",");

        ColumnJoin columnJoin = new ColumnJoin(Arrays.asList(6, 2, 1));
        String[] actualValue = columnJoin.apply(inputRecord);

        assertEquals("FirstName,MiddleName,One,Two,Three 732 LastName", StringUtils.join(actualValue,","));
    }

    @Test
    public void shouldMargeTheGivenColumnsAndPlaceTheResultAtCurrectPosition() {
        String[] inputRecord = "FirstName,LastName,732,MiddleName,One,Two,Three".split(",");

        ColumnJoin columnJoin = new ColumnJoin(Arrays.asList(4, 6, 1));
        String[] actualValue = columnJoin.apply(inputRecord);

        assertEquals("FirstName,732,MiddleName,One Three LastName,Two", StringUtils.join(actualValue,","));
    }
}