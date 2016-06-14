package org.apache.prepbuddy.smoothers;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/**
 * A smoothing method which smooths data based on Simple Moving Average which is the unweighted mean of
 * the previous n data. this method ensure that variations in the mean are aligned
 * with the variations in the data rather than being shifted in time.
 */
public class SimpleMovingAverageMethod extends SmoothingMethod {

    private int windowSize;

    public SimpleMovingAverageMethod(int window) {
        this.windowSize = window;
    }

    @Override
    public JavaRDD<Double> smooth(JavaRDD<String> singleColumnDataset) {
        JavaRDD<Double> duplicateRdd = prepare(singleColumnDataset, windowSize);
        JavaRDD<Double> smoothed = duplicateRdd.mapPartitions(new FlatMapFunction<Iterator<Double>, Double>() {
            @Override
            public Iterable<Double> call(Iterator<Double> iterator) throws Exception {
                List<Double> movingAverages = new ArrayList<>();
                SimpleSlidingWindow slidingWindow = new SimpleSlidingWindow(windowSize);
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
