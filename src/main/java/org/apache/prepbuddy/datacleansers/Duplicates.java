package org.apache.prepbuddy.datacleansers;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;

public class Duplicates implements Serializable {
    public JavaRDD apply(JavaRDD inputRecords) {
        JavaPairRDD recordOnePair = inputRecords.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String record) throws Exception {
                return new Tuple2<>(record, 1);
            }
        });
        JavaPairRDD recordCountPairRDD = recordOnePair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer accumulator, Integer current) throws Exception {
                return accumulator + current;
            }
        });

        JavaPairRDD duplicateRecords = recordCountPairRDD.filter(new Function<Tuple2<String, Integer>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Integer> recordCountPair) throws Exception {
                return recordCountPair._2() != 1;
            }
        });

        return duplicateRecords.keys();
    }
}
