package org.apache.prepbuddy.examples;

import org.apache.prepbuddy.qualityanalyzers.AnalysisPlan;
import org.apache.prepbuddy.qualityanalyzers.AnalysisResult;
import org.apache.prepbuddy.qualityanalyzers.FileType;
import org.apache.prepbuddy.rdds.AnalyzableRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Arrays;

public class AnalyzeData {
    public static void main(String[] args) {
        String filePath = args[0];
        int columnToAnalyze = Integer.parseInt(args[1]);

        SparkConf conf = new SparkConf().setAppName("Quality Analysis");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> initialRDD = sc.textFile(filePath);

        AnalyzableRDD analyzableRDD = new AnalyzableRDD(initialRDD, FileType.TSV);
        AnalysisPlan analysisPlan = new AnalysisPlan(columnToAnalyze, Arrays.asList("\\N", "N/A"));

        AnalysisResult analysisResult = analyzableRDD.analyzeColumns(analysisPlan);

        System.out.println(analysisResult.dataType());
        System.out.println(analysisResult.percentageOfMissingValues());
    }
}
