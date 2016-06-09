package org.apache.prepbuddy.smoothingops;

import org.apache.spark.api.java.JavaRDD;

public class ExponentialAverage extends MovingAverage {
    private ExponentialSlidingWindow slidingWindow;
    private final double windowSize = 2;

    public ExponentialAverage(double weightFactor) {
        slidingWindow = new ExponentialSlidingWindow(weightFactor);
    }

    @Override
    public JavaRDD<Double> smooth(JavaRDD<String> singleColumnDataset) {
        return null;
    }
}
