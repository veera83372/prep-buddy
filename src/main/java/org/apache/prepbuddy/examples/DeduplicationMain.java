package org.apache.prepbuddy.examples;

import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Date;

public class DeduplicationMain {
    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("--> File Path Need To Be Specified with number of partition");
            System.exit(0);
        }

        SparkConf conf = new SparkConf().setAppName("DuplicationHandler");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String filePath = args[0];
        JavaRDD<String> csvInput = sc.textFile(filePath);
        TransformableRDD transformableRDD = new TransformableRDD(csvInput);

        TransformableRDD deduplicate = transformableRDD.deduplicate();
        deduplicate.saveAsTextFile("s3://twi-analytics-sandbox/job-output/unique-" + new Date().toString());

        sc.close();
    }
}
