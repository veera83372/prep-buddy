package org.apache.prepbuddy.smoothingops;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

import java.util.Iterator;

public class ExponentialAverage extends SmoothingMethod {
    private ExponentialSlidingWindow slidingWindow;
    private final int windowSize = 2;

    public ExponentialAverage(double weightFactor) {
        slidingWindow = new ExponentialSlidingWindow(weightFactor);
    }

    @Override
    public JavaRDD<Double> smooth(JavaRDD<String> singleColumnDataset) {
        JavaRDD<Double> doubleJavaRDD = singleColumnDataset.mapPartitionsWithIndex(new AccumulatingFunction(), true);

        return doubleJavaRDD;
    }

    private static class AccumulatingFunction implements Function2<Integer, Iterator<String>, Iterator<Double>> {

        @Override
        public Iterator<Double> call(Integer partitionIndex, Iterator<String> v2) throws Exception {

            return null;
        }
    }
}
