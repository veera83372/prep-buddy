package org.apache.prepbuddy.smoothingops;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class ExponentialAverage extends SmoothingMethod {
    private ExponentialSlidingWindow slidingWindow;
    private final int windowSize = 2;

    public ExponentialAverage(double weightFactor) {
        slidingWindow = new ExponentialSlidingWindow(weightFactor);
    }

    @Override
    public JavaRDD<Double> smooth(JavaRDD<String> singleColumnDataset) {
        JavaRDD<Double> duplicatedRdd = prepare(singleColumnDataset, windowSize);
        return duplicatedRdd.mapPartitions(new FlatMapFunction<Iterator<Double>, Double>() {
            @Override
            public Iterable<Double> call(Iterator<Double> iterator) throws Exception {
                List<Double> weightedMovingAverages = new ArrayList<>();
                while (iterator.hasNext()) {
                    Double value = iterator.next();
                    slidingWindow.add(value);
                    if (slidingWindow.isFull()) {
                        Double average = slidingWindow.average();
                        weightedMovingAverages.add(average);
                    }
                }
                return weightedMovingAverages;
            }
        });
    }
}
