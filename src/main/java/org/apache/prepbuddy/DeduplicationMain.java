package org.apache.prepbuddy;

import org.apache.prepbuddy.datacleansers.Deduplication;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.Date;

public class DeduplicationMain {
    public static void main(String[] args) {
        if(args.length == 0) {
            System.out.println("--> File Path Need To Be Specified with number of partition");
            System.exit(0);
        }

        SparkConf conf = new SparkConf().setAppName("Deduplication");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String filePath = args[0];
        JavaRDD<String> csvInput = sc.textFile(filePath);

        JavaRDD<String> transformedRdd = new Deduplication().apply(csvInput);
        transformedRdd.saveAsTextFile("s3://twi-analytics-sandbox/job-output/unique-"+new Date().toString());

        sc.close();
    }
}
