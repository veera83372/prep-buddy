package org.apache.prepbuddy.transformations.deduplication;

import org.apache.prepbuddy.transformations.DataTransformation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

public class DeduplicationTransformation implements DataTransformation {

    private String inputPath;
    private String outputPath;

    public DeduplicationTransformation(String inputFile, String outputFile) {
        this.inputPath = inputFile;
        this.outputPath = outputFile;
    }

    @Override
    public void apply(DeduplicationInput input) {
        SparkConf sparkConf = new SparkConf().setAppName("Deduplication Transformation");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(ctx);
        DataFrame frame = sqlContext.read().text(inputPath);
        DataFrame distinctFrame = frame.distinct();
        distinctFrame.save(outputPath, SaveMode.Overwrite);
    }

}
