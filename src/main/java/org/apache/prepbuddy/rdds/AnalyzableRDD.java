package org.apache.prepbuddy.rdds;

import org.apache.commons.lang.StringUtils;
import org.apache.prepbuddy.qualityanalyzers.AnalysisPlan;
import org.apache.prepbuddy.qualityanalyzers.AnalysisResult;
import org.apache.prepbuddy.qualityanalyzers.DataType;
import org.apache.prepbuddy.qualityanalyzers.FileType;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

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
        int columnIndex = plan.columnIndex();
        DataType dataType = inferType(columnIndex);
        Map<Integer, Integer> missingDataReport = countMissingValues(plan.columnIndex(), plan.missingHints());
        AnalysisResult result = new AnalysisResult(columnIndex, dataType, numberOfRows, missingDataReport);
        return result;
    }

    private Map<Integer, Integer> countMissingValues(final int columnIndex, final List<String> missingHints) {
        JavaPairRDD<Integer, Integer> intermediate = this.mapToPair(new PairFunction<String, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(String record) throws Exception {
                String[] columnValues = fileType.parseRecord(record);
                Integer missingCount = 0;
                if (hasMissingData(columnValues, columnIndex, missingHints)) {
                    missingCount = 1;
                }
                return new Tuple2<>(columnIndex, missingCount);
            }

            private boolean hasMissingData(String[] columnValues, int columnIndex, List<String> missingHints) {
                if (columnIndex < columnValues.length) {
                    String columnValue = columnValues[columnIndex];
                    return StringUtils.isBlank(columnValue) || missingHints.contains(columnValue);
                }
                return true;
            }

        });
        JavaPairRDD<Integer, Integer> missingDataSummary = intermediate.reduceByKey(new Function2<Integer, Integer, Integer>() {

            @Override
            public Integer call(Integer accumulator, Integer missingCount) throws Exception {
                return accumulator + missingCount;
            }
        });
        return missingDataSummary.collectAsMap();
    }
}
