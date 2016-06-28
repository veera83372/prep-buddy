package org.apache.prepbuddy.rdds;

import org.apache.prepbuddy.exceptions.ApplicationException;
import org.apache.prepbuddy.exceptions.ErrorMessages;
import org.apache.prepbuddy.qualityanalyzers.BaseDataType;
import org.apache.prepbuddy.qualityanalyzers.DataType;
import org.apache.prepbuddy.qualityanalyzers.FileType;
import org.apache.prepbuddy.qualityanalyzers.TypeAnalyzer;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.DoubleFunction;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public abstract class AbstractRDD extends JavaRDD<String> {
    public static final int DEFAULT_SAMPLE_SIZE = 1000;
    protected FileType fileType;
    private List<String> sampleRecords;

    public AbstractRDD(JavaRDD<String> rdd, FileType fileType) {
        super(rdd.rdd(), rdd.rdd().elementClassTag());
        this.fileType = fileType;
        sampleRecords = this.takeSample(false, DEFAULT_SAMPLE_SIZE);
    }

    /**
     * Returns a inferred DataType of given column index
     *
     * @param columnIndex Column index
     * @return DataType
     */
    public DataType inferType(final int columnIndex) {
        validateColumnIndex(columnIndex);
        List<String> columnSamples = sampleColumnValues(columnIndex);
        TypeAnalyzer typeAnalyzer = new TypeAnalyzer(columnSamples);
        return typeAnalyzer.getType();
    }

    protected void validateColumnIndex(int... columnIndexes) {
        int size = getNumberOfColumns();
        for (int index : columnIndexes) {
            if (index < 0 || size <= index)
                throw new ApplicationException(ErrorMessages.COLUMN_INDEX_OUT_OF_BOUND);
        }
    }

    protected boolean isNumericColumn(int columnIndex) {
        List<String> columnSamples = sampleColumnValues(columnIndex);
        BaseDataType baseType = BaseDataType.getBaseType(columnSamples);
        return baseType.equals(BaseDataType.NUMERIC);
    }

    public List<String> sampleColumnValues(int columnIndex) {
        List<String> columnSamples = new LinkedList<>();
        for (String record : sampleRecords) {
            String[] strings = fileType.parseRecord(record);
            columnSamples.add(strings[columnIndex]);
        }
        return columnSamples;
    }

    /**
     * Returns the number of columns in this RDD
     *
     * @return int
     */
    public int getNumberOfColumns() {
        Map<Integer, Integer> columnLengthAndCount = new HashMap<>();
        for (String row : sampleRecords) {
            int columnLength = fileType.parseRecord(row).length;
            if (columnLengthAndCount.containsKey(columnLength)) {
                Integer count = columnLengthAndCount.get(columnLength);
                columnLengthAndCount.put(columnLength, count + 1);
            } else {
                columnLengthAndCount.put(columnLength, 1);
            }
        }
        return getHighestCountKey(columnLengthAndCount);
    }

    private int getHighestCountKey(Map<Integer, Integer> lengthWithCount) {
        Integer highest = 0;
        Integer highestKey = 0;
        for (Integer key : lengthWithCount.keySet()) {
            Integer count = lengthWithCount.get(key);
            if (highest < count) {
                highest = count;
                highestKey = key;
            }
        }
        return highestKey;
    }

    /**
     * Returns a JavaDoubleRdd of given column index
     *
     * @param columnIndex Column index
     * @return JavaDoubleRDD
     */
    public JavaDoubleRDD toDoubleRDD(final int columnIndex) {
        if (!isNumericColumn(columnIndex))
            throw new ApplicationException(ErrorMessages.COLUMN_VALUES_ARE_NOT_NUMERIC);

        return this.mapToDouble(new DoubleFunction<String>() {
            @Override
            public double call(String row) throws Exception {
                String[] recordAsArray = fileType.parseRecord(row);
                String columnValue = recordAsArray[columnIndex];
                if (!columnValue.trim().isEmpty())
                    return Double.parseDouble(recordAsArray[columnIndex]);
                return 0;
            }
        });
    }
}
