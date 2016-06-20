package org.apache.prepbuddy.rdds;

import org.apache.prepbuddy.qualityanalyzers.AnalysisResult;
import org.apache.prepbuddy.qualityanalyzers.DataType;
import org.apache.prepbuddy.qualityanalyzers.FileType;
import org.apache.spark.api.java.JavaRDD;

public class AnalyzableRDD extends AbstractRDD {

    public AnalyzableRDD(JavaRDD<String> rdd, FileType fileType) {
        super(rdd, fileType);
    }

    public AnalyzableRDD(JavaRDD<String> rdd) {
        super(rdd, FileType.CSV);
    }


    public AnalysisResult analyzeColumn(int columnIndex) {
        DataType dataType = inferType(columnIndex);
        AnalysisResult result = new AnalysisResult(columnIndex, dataType);

        return result;
    }
}
