package org.apache.prepbuddy;

import org.apache.prepbuddy.datacleansers.Deduplication;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.List;

public class DeduplicationMain {
    public static void main(String[] args) {
        if(args.length == 0) {
            System.out.println("--> File Path Need To Be Specified");
            System.exit(1);
        }
        SparkConf conf = new SparkConf().setAppName("Deduplication");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String filePath = args[0];
        JavaRDD<String> csvInput = sc.textFile(filePath);
        long numberOfRecordsInInput = csvInput.count();

        Deduplication deduplication = new Deduplication();
        JavaRDD transformedRecord = deduplication.apply(csvInput);
        long numberOfRecordInTransformed = transformedRecord.count();

        long eleminationCount = numberOfRecordsInInput - numberOfRecordInTransformed;
        System.out.println("-->>> Total " + eleminationCount + " duplicate records are removed");

        List<String> sampleRecord = transformedRecord
                .sample(false,0.001)
                .collect();

        System.out.println("Sample Dataset: ");
        for (String record : sampleRecord) {
            System.out.println(record);
        }
        sc.close();
    }
}
