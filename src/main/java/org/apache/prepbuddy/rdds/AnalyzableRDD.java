package org.apache.prepbuddy.rdds;

import org.apache.commons.lang.StringUtils;
import org.apache.prepbuddy.qualityanalyzers.*;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;

public class AnalyzableRDD extends AbstractRDD {

    private long numberOfRows;

    public AnalyzableRDD(JavaRDD<String> rdd, FileType fileType) {
        super(rdd, fileType);
        //todo : come back and look at the possibility of using countApprox
        numberOfRows = rdd.count();
    }

    public AnalyzableRDD(JavaRDD<String> rdd) {
        this(rdd, FileType.CSV);
    }

    public DatasetInsights analyzeColumns(final AnalysisPlan plan) {
        List<Integer> columnIndexes = plan.columnIndexes();
        HashMap<Integer, ColumnInsight> columnInsights = new HashMap<>();
        for (Integer columnIndex : columnIndexes) {
            DataType dataType = inferType(columnIndex);
            int missingValueCount = countMissingValues(columnIndex, plan.missingHints());
            columnInsights.put(columnIndex, new ColumnInsight(columnIndex, dataType, missingValueCount));
        }
        DatasetInsights result = new DatasetInsights(numberOfRows, columnInsights);
        return result;
    }

    private int countMissingValues(final int columnIndex, final List<String> missingHints) {
        JavaRDD<Integer> missingDataCount = this.map(new Function<String, Integer>() {
            @Override
            public Integer call(String record) throws Exception {
                String[] columnValues = fileType.parseRecord(record);
                Integer missingCount = 0;
                if (hasMissingData(columnValues, columnIndex, missingHints)) {
                    missingCount = 1;
                }
                return missingCount;
            }

            private boolean hasMissingData(String[] columnValues, int columnIndex, List<String> missingHints) {
                if (columnIndex < columnValues.length) {
                    String columnValue = columnValues[columnIndex];
                    return StringUtils.isBlank(columnValue) || missingHints.contains(columnValue);
                }
                return true;
            }

        });
        Integer totalMissingValue = missingDataCount.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer accumulator, Integer currentValue) throws Exception {
                return accumulator + currentValue;
            }
        });
        return totalMissingValue;
    }

    public Tuple2<double[], long[]> histogram(final int columnIndex) {
        JavaDoubleRDD doubleRDD = this.toDoubleRDD(columnIndex);
        Double max = doubleRDD.max();
        Double min = doubleRDD.min();
        double noOfBins = (max - min) / Math.sqrt(numberOfRows);
        return doubleRDD.histogram((int)noOfBins);
    }
}
