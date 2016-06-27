package org.apache.prepbuddy.examples;

import org.apache.prepbuddy.qualityanalyzers.AnalysisPlan;
import org.apache.prepbuddy.qualityanalyzers.AnalysisResult;
import org.apache.prepbuddy.qualityanalyzers.FileType;
import org.apache.prepbuddy.rdds.AnalyzableRDD;
import org.apache.prepbuddy.utils.Range;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;
import java.util.Map;

public class FunctionalTestSuite {
    public static void main(String[] args) {
        String filePath = args[0];
        SparkConf conf = new SparkConf().setAppName("Functional Test Suite");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> initialRDD = sc.textFile(filePath);

        AnalyzableRDD analyzableRDD = new AnalyzableRDD(initialRDD, FileType.TSV);
        Range range = new Range(0, analyzableRDD.getNumberOfColumns() - 1);
        AnalysisPlan analysisPlan = new AnalysisPlan(range, Arrays.asList("\\N", "N/A"));
        AnalysisResult analysisResult = analyzableRDD.analyzeColumns(analysisPlan);

        Map<Integer, Double> percentageOfMissingValues = analysisResult.percentageOfMissingValues();
        for (Integer columnIndex : percentageOfMissingValues.keySet()) {
            System.out.println("Percentage Of Missing Value at " + columnIndex + " is " + percentageOfMissingValues.get(columnIndex));
        }
    }
}
