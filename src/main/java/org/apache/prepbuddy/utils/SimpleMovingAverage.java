package org.apache.prepbuddy.utils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;

public class SimpleMovingAverage implements Serializable {

    private int window;

    public SimpleMovingAverage(int window) {
        this.window = window;
    }

    public JavaRDD<String> smooth(JavaRDD<String> dataset) {
        JavaRDD<String> duplicateRdd = SmoothingPreparation.prepare(dataset, window);
        JavaRDD<String> smoothed = duplicateRdd.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            @Override
            public Iterable<String> call(Iterator<String> iterator) throws Exception {
                ArrayList<String> averages = new ArrayList<>();
                SimpleMovingAverageCalculator numberClosure = new SimpleMovingAverageCalculator(window);
                while (iterator.hasNext()) {
                    Double value = Double.parseDouble(iterator.next());
                    numberClosure.add(value);
                    if (numberClosure.isFull()) {
                        String average = String.valueOf(numberClosure.average());
                        averages.add(average);
                    }
                }
                return averages;
            }
        });
        return smoothed;
    }
}
