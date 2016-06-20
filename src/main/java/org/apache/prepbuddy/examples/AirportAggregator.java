package org.apache.prepbuddy.examples;

import org.apache.prepbuddy.qualityanalyzers.FileType;
import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;

public class AirportAggregator implements Serializable {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("AirportAggregator");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String filePath = args[0];
        JavaRDD<String> csvInput = sc.textFile(filePath);
        TransformableRDD transformableRDD = new TransformableRDD(csvInput);

        JavaPairRDD<String, String> airportListByCode = transformableRDD.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String record) throws Exception {
                String[] recordAsArray = FileType.CSV.parseRecord(record);
                return new Tuple2<String, String>(recordAsArray[1], recordAsArray[0]);
            }
        });
        JavaPairRDD<String, Iterable<String>> keys = airportListByCode.groupByKey();
        keys.saveAsTextFile("data/airportByCode");
    }
}
