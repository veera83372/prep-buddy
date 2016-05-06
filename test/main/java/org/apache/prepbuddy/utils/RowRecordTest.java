package org.apache.prepbuddy.utils;

import org.apache.prepbuddy.coreops.ColumnTransformation;
import org.apache.prepbuddy.datacleansers.Imputation;
import org.apache.prepbuddy.transformation.ColumnSplitter;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class RowRecordTest {

    @Test
    public void ShouldReturnsBackANewRowRecordAfterSplittingThePreviousRecordAtGivenPosition() {
        RowRecord record = new RowRecord("FirstName LastName,Something Else".split(","));
        ColumnSplitter splitBySpace = new ColumnSplitter(0, " ");

        RowRecord expected = new RowRecord("FirstName,LastName,Something Else".split(","));
        RowRecord actual = record.getModifiedRecord(splitBySpace);
        assertTrue(expected.equals(actual));
    }

    @Test
    public void ShouldReturnsBackANewRowRecordAfterDoingSomeColumnTransformations() {
        RowRecord record = new RowRecord("FirstName LastName,Something Else,".split(",",-1));

        ColumnTransformation columnTransformation = new ColumnTransformation(2);
        columnTransformation.setupImputation(new Imputation() {
            @Override
            protected String handleMissingData(String[] record) {
                return "Male";
            }
        });


        RowRecord expected = new RowRecord("FirstName LastName,Something Else,Male".split(",",-1));
        RowRecord actual = record.getModifiedRecord(columnTransformation);
        assertTrue(expected.equals(actual));
    }


}