package org.apache.prepbuddy.datasmoothers;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SimpleMovingAverage extends MovingAverage {

    private int windowSize;

    public SimpleMovingAverage(int window) {
        this.windowSize = window;
    }

    @Override
    public JavaRDD<Double> smooth(JavaRDD<String> singleColumnDataset) {
        JavaRDD<Double> duplicateRdd = prepare(singleColumnDataset, windowSize);
        JavaRDD<Double> smoothed = duplicateRdd.mapPartitions(new FlatMapFunction<Iterator<Double>, Double>() {
            @Override
            public Iterable<Double> call(Iterator<Double> iterator) throws Exception {
                List<Double> movingAverages = new ArrayList<>();
                SlidingWindow slidingWindow = new SlidingWindow(windowSize);
                while (iterator.hasNext()) {
                    Double value = iterator.next();
                    slidingWindow.add(value);
                    if (slidingWindow.isFull()) {
                        Double average = slidingWindow.average();
                        movingAverages.add(average);
                    }
                }
                return movingAverages;
            }
        });
        return smoothed;
    }
}
