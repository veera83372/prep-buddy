package org.apache.prepbuddy.utils;

import org.apache.prepbuddy.transformation.ColumnSplitter;
import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;

public class RowRecordTest {

    @Test
    public void ShouldReturnsBackANewRowRecordAfterSplittingThePreviousRecordAtGivenPosition() {
//        RowRecord record = new RowRecord("FirstName LastName,Something Else".splitColumn(","));
        ColumnSplitter splitBySpace = new ColumnSplitter(" ", false);

//        RowRecord expected = new RowRecord("FirstName,LastName,Something Else".splitColumn(","));
//        RowRecord actual = record.getModifiedRecord(splitBySpace, 0);
        String[] actual = splitBySpace.apply("FirstName LastName,Something Else".split(","), 0);
        String[] expected = "FirstName,LastName,Something Else".split(",");

        assertArrayEquals(expected,actual);
    }
}