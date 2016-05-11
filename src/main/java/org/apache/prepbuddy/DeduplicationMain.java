package org.apache.prepbuddy;

import org.apache.prepbuddy.datacleansers.Deduplication;
import org.apache.prepbuddy.filetypes.FileType;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;


import java.io.Serializable;

public class DeduplicationMain implements Serializable {
    public static void main(String[] args) {
        if(args.length == 0) {
            System.out.println("--> File Path Need To Be Specified");
            System.exit(0);
        }
        SparkConf conf = new SparkConf().setAppName("Deduplication");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String filePath = args[0];
        JavaRDD<String> csvInput = sc.textFile(filePath);
        long numberOfRecordsInInput = csvInput.count();

        Deduplication deduplication = new Deduplication();
        JavaRDD transformedRecord = deduplication.apply(csvInput, FileType.CSV);
        long numberOfRecordInTransformed = transformedRecord.count();

        long elimination = numberOfRecordsInInput - numberOfRecordInTransformed;
        System.out.println("-->>> Total " + elimination + " duplicate records are removed");

        sc.close();
    }
}
