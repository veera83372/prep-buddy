package org.apache.prepbuddy.utils;

import org.apache.prepbuddy.transformation.TransformationOperation;

import java.util.Arrays;

public class RowRecord {
    private String[] columnValues;

    public RowRecord(final String[] columnValues) {
        this.columnValues = columnValues;
    }

    public RowRecord getModifiedRecord(TransformationOperation operation){
        String[] transformedValues = operation.apply(columnValues);
        return new RowRecord(transformedValues);
    };

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof RowRecord)) return false;

        RowRecord rowRecord = (RowRecord) o;

        return Arrays.equals(columnValues, rowRecord.columnValues);

    }
}
