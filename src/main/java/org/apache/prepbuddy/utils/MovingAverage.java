package org.apache.prepbuddy.utils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

public class MovingAverage implements Serializable {

    private int window;

    public MovingAverage(int window) {

        this.window = window;
    }

    public JavaRDD<String> smooth(JavaRDD<String> dataset) {
        JavaRDD<String> duplicateRdd = SmoothingPreparation.prepare(dataset, window);
        duplicateRdd.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            @Override
            public Iterable<String> call(Iterator<String> iterator) throws Exception {
                ArrayList<String> averages = new ArrayList<>();
                return averages;
            }
        });
        return duplicateRdd;
    }
}
