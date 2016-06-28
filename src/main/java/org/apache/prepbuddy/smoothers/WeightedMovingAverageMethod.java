package org.apache.prepbuddy.smoothers;

import org.apache.prepbuddy.exceptions.ApplicationException;
import org.apache.prepbuddy.exceptions.ErrorMessages;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A smoothing method which smooths data based on Weighted Moving Average method that is any
 * average that has multiplying factors to give different weights to data at
 * different positions in the sampleColumnValues window.
 */
public class WeightedMovingAverageMethod extends SmoothingMethod {
    private int windowSize;
    private WeightedSlidingWindow slidingWindow;

    public WeightedMovingAverageMethod(int windowSize, final Weights weights) {
        this.windowSize = windowSize;
        if (weights.size() != windowSize)
            throw new ApplicationException(ErrorMessages.WINDOW_SIZE_AND_WEIGHTS_SIZE_NOT_MATCHING);
        slidingWindow = new WeightedSlidingWindow(windowSize, weights);
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
