package org.apache.prepbuddy.utils;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SimpleMovingAverage implements Serializable {

    private int windowSize;

    public SimpleMovingAverage(int window) {
        this.windowSize = window;
    }

    public JavaRDD<String> smooth(JavaRDD<String> singleColumnDataset) {
        JavaRDD<String> duplicateRdd = SmoothingPreparation.prepare(singleColumnDataset, windowSize);
        JavaRDD<String> smoothed = duplicateRdd.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {
            @Override
            public Iterable<String> call(Iterator<String> iterator) throws Exception {
                List<String> movingAverages = new ArrayList<>();
                SlidingWindow slidingWindow = new SlidingWindow(windowSize);
                while (iterator.hasNext()) {
                    Double value = Double.parseDouble(iterator.next());
                    slidingWindow.add(value);
                    if (slidingWindow.isFull()) {
                        String average = String.valueOf(slidingWindow.average());
                        movingAverages.add(average);
                    }
                }
                return movingAverages;
            }
        });
        return smoothed;
    }
}
