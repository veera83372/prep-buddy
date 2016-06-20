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

import java.util.*;

public abstract class AbstractRDD extends JavaRDD<String> {
    protected FileType fileType;

    public AbstractRDD(JavaRDD<String> rdd, FileType fileType) {
        super(rdd.rdd(), rdd.rdd().elementClassTag());
        this.fileType = fileType;
    }


    /**
     * Returns a inferred DataType of given column index
     *
     * @param columnIndex The column where the inference will be done.
     * @return DataType
     */
    public DataType inferType(final int columnIndex) {
        validateColumnIndex(columnIndex);
        List<String> columnSamples = sample(columnIndex);
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
        List<String> columnSamples = sample(columnIndex);
        BaseDataType baseType = BaseDataType.getBaseType(columnSamples);
        return baseType.equals(BaseDataType.NUMERIC);
    }

    public List<String> sample(int columnIndex, int sampleSize) {
        List<String> rowSamples = this.takeSample(false, sampleSize);
        List<String> columnSamples = new LinkedList<>();
        for (String row : rowSamples) {
            String[] strings = fileType.parseRecord(row);
            columnSamples.add(strings[columnIndex]);
        }
        return columnSamples;
    }

    private List<String> sample(int columnIndex) {
        return sample(columnIndex, 100);
    }

    /**
     * Returns the number of columns in this RDD
     *
     * @return int
     */
    public int getNumberOfColumns() {
        List<String> sample = this.takeSample(false, 5);
        Map<Integer, Integer> noOfColsAndCount = new HashMap<>();
        for (String row : sample) {
            Set<Integer> lengths = noOfColsAndCount.keySet();
            int rowLength = fileType.parseRecord(row).length;
            if (!lengths.contains(rowLength))
                noOfColsAndCount.put(rowLength, 1);
            else {
                Integer count = noOfColsAndCount.get(rowLength);
                noOfColsAndCount.put(rowLength, count + 1);
            }
        }
        return getHighestCountKey(noOfColsAndCount);
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
     * @param columnIndex
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
