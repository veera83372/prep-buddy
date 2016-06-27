package org.apache.prepbuddy.examples;

import org.apache.prepbuddy.qualityanalyzers.AnalysisPlan;
import org.apache.prepbuddy.qualityanalyzers.AnalysisResult;
import org.apache.prepbuddy.qualityanalyzers.FileType;
import org.apache.prepbuddy.rdds.AnalyzableRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class FunctionalTestSuite {
    public static void main(String[] args) {
        String filePath = args[0];
        int numberOfColumns = Integer.parseInt(args[1]);
        SparkConf conf = new SparkConf().setAppName("Functional Test Suite");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> initialRDD = sc.textFile(filePath);

        AnalyzableRDD analyzableRDD = new AnalyzableRDD(initialRDD, FileType.TSV);

        for (int index = 0; index < numberOfColumns; index++) {
            analyzableRDD.cache();
            AnalysisPlan analysisPlan = new AnalysisPlan(index, Arrays.asList("\\N", "N/A"));
            AnalysisResult analysisResult = analyzableRDD.analyzeColumns(analysisPlan);
            System.out.printf("%s : %s\n", index, analysisResult.dataType());
        }
    }
}
