package org.apache.prepbuddy.smoothers;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public abstract class SmoothingMethod implements Serializable {

    public JavaRDD<Double> prepare(JavaRDD<String> singleColumnDataset, final int windowSize) {
        JavaRDD<Tuple2<Integer, String>> duplicateRdd = singleColumnDataset.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<Tuple2<Integer, String>>>() {
            @Override
            public Iterator<Tuple2<Integer, String>> call(Integer index, Iterator<String> iterator) throws Exception {

                List<Tuple2<Integer, String>> list = new ArrayList<>();
                while (iterator.hasNext()) {
                    String next = iterator.next();
                    Tuple2<Integer, String> tuple = new Tuple2<>(index, next);
                    list.add(tuple);
                }
                if (index != 0) {
                    for (int i = 0; i < windowSize - 1; i++) {
                        Tuple2<Integer, String> toDuplicate = list.get(i);
                        list.add(new Tuple2<>(toDuplicate._1() - 1, toDuplicate._2()));
                    }
                }
                return list.iterator();
            }
        }, true);
        return keyPartition(duplicateRdd).map(new Function<Tuple2<Integer, String>, Double>() {
            @Override
            public Double call(Tuple2<Integer, String> tuple) throws Exception {
                return Double.parseDouble(tuple._2());
            }
        });
    }

    private JavaPairRDD<Integer, String> keyPartition(JavaRDD<Tuple2<Integer, String>> tupleJavaRDD) {
        return tupleJavaRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<Integer, String> tuple) throws Exception {
                return tuple;
            }
        }).partitionBy(new KeyPartitioner(tupleJavaRDD.getNumPartitions()));
    }

    public abstract JavaRDD<Double> smooth(JavaRDD<String> singleColumnDataset);
}
