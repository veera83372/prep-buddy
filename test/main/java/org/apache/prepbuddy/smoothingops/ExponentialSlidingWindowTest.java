package org.apache.prepbuddy.smoothingops;

import org.apache.prepbuddy.SparkTestCase;
import org.junit.Assert;
import org.junit.Test;

import java.text.DecimalFormat;

public class ExponentialSlidingWindowTest extends SparkTestCase {

    @Test
    public void shouldAbleToGiveExponentialAverage() {
        ExponentialSlidingWindow slidingWindow = new ExponentialSlidingWindow(0.2);
        slidingWindow.add(23);
        slidingWindow.add(40);

        Double expected = 26.4;
        Double actual = Double.parseDouble(new DecimalFormat("##.##").format(slidingWindow.average()));
        Assert.assertEquals(expected, actual);

        slidingWindow.add(25);

        Double expected1 = 26.12;
        Double actual1 = Double.parseDouble(new DecimalFormat("##.##").format(slidingWindow.average()));
        Assert.assertEquals(expected1, actual1);
    }
}