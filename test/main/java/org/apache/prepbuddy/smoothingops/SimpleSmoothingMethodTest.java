package org.apache.prepbuddy.smoothingops;

import org.apache.prepbuddy.SparkTestCase;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class SimpleSmoothingMethodTest extends SparkTestCase {
    @Test
    public void shouldSmoothDataSetBySimpleMovingAverage() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "3", "4", "5", "6", "7", "8", "9", "10", "11", "12", "13", "14"
        ), 3);
        SimpleMovingAverageMethod movingAverage = new SimpleMovingAverageMethod(3);
        JavaRDD<Double> rdd = movingAverage.smooth(initialDataset);

        Double expected = 4.0;
        Assert.assertEquals(expected, rdd.first());
    }


}
