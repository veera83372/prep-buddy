package org.apache.prepbuddy.rdds;

import org.apache.prepbuddy.qualityanalyzers.AnalysisResult;
import org.apache.prepbuddy.qualityanalyzers.FileType;
import org.apache.spark.api.java.JavaRDD;

import java.util.LinkedList;
import java.util.List;

public class AnalyzableRDD extends JavaRDD<String> {
    private FileType fileType;

    public AnalyzableRDD(JavaRDD<String> rdd, FileType fileType) {
        super(rdd.rdd(), rdd.rdd().elementClassTag());
        this.fileType = fileType;
    }

    public AnalyzableRDD(JavaRDD<String> rdd) {
        this(rdd, FileType.CSV);
    }


    public List<String> sample(int columnIndex) {
        List<String> columnSamples = sample(columnIndex, 100);
        return columnSamples;
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


    public AnalysisResult analyzeColumn(int columnIndex) {
        AnalysisResult report = new AnalysisResult(columnIndex);

        return report;
    }
}
