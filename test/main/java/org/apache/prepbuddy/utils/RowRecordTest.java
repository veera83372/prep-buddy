package org.apache.prepbuddy.utils;

import org.apache.prepbuddy.transformation.SplitByDelimiter;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class RowRecordTest {

    @Test
    public void ShouldReturnsBackANewRowRecordAfterSplittingThePreviousRecordAtGivenPosition() {
        SplitByDelimiter splitBySpace = new SplitByDelimiter(" ", false);

        String[] actual = splitBySpace.apply("FirstName LastName,Something Else".split(","), 0);
        String[] expected = "FirstName,LastName,Something Else".split(",");

        assertArrayEquals(expected,actual);
    }
}