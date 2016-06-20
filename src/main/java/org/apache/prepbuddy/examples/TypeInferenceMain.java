package org.apache.prepbuddy.examples;

import org.apache.prepbuddy.analyzers.DataType;
import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

public class TypeInferenceMain implements Serializable {
    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("--> File Path Need To Be Specified");
            System.exit(0);
        }

        SparkConf conf = new SparkConf().setAppName("Type Inference").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String filePath = args[0];
        JavaRDD<String> csvInput = sc.textFile(filePath);

        TransformableRDD transformableRDD = new TransformableRDD(csvInput);
        DataType dataType = transformableRDD.inferType(0);
        System.out.println("dataType = " + dataType);

        sc.close();
    }
}
