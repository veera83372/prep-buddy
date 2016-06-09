package org.apache.prepbuddy.datasmoothers;

import org.apache.prepbuddy.SparkTestCase;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

import java.text.DecimalFormat;
import java.util.Arrays;

public class WeightedMovingAverageTest extends SparkTestCase {
    @Test
    public void shouldSmoothDataByWeightedMovingAverage() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "10", "12", "16", "13", "17", "19", "15", "20", "22", "19", "21", "19"
        ), 3);

        Weights weights = new Weights(3);
        weights.add(0.166);
        weights.add(0.333);
        weights.add(0.5);

        WeightedMovingAverage movingAverage = new WeightedMovingAverage(3, weights);
        JavaRDD<Double> rdd = movingAverage.smooth(initialDataset);

        Double expected = 13.66;
        Double actual = Double.parseDouble(new DecimalFormat("##.##").format(rdd.first()));
        Assert.assertEquals(expected, actual);
    }
}