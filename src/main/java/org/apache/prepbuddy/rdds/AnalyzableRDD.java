package org.apache.prepbuddy.rdds;

import org.apache.commons.lang.StringUtils;
import org.apache.prepbuddy.qualityanalyzers.AnalysisPlan;
import org.apache.prepbuddy.qualityanalyzers.AnalysisResult;
import org.apache.prepbuddy.qualityanalyzers.DataType;
import org.apache.prepbuddy.qualityanalyzers.FileType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

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


    public AnalysisResult analyzeColumns(final AnalysisPlan plan) {
        List<Integer> columnIndexes = plan.columnIndexes();
        Map<Integer, DataType> dataTypeReport = new HashMap<>();
        Map<Integer, Integer> missingDataReport = new HashMap<>();

        for (Integer columnIndex : columnIndexes) {
            dataTypeReport.put(columnIndex, inferType(columnIndex));
            missingDataReport.put(columnIndex, countMissingValues(columnIndex, plan.missingHints()));
        }
        AnalysisResult result = new AnalysisResult(numberOfRows, dataTypeReport, missingDataReport);
        return result;
    }

    private int countMissingValues(final int columnIndex, final List<String> missingHints) {
        JavaRDD<Integer> intermediate = this.map(new Function<String, Integer>() {
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
        Integer totalMissingValue = intermediate.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer accumulator, Integer currentValue) throws Exception {
                return accumulator + currentValue;
            }
        });
        return totalMissingValue;
    }
}
