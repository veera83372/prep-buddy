package org.apache.prepbuddy.datasmoothers;

import org.apache.prepbuddy.SparkTestCase;
import org.junit.Test;

import java.text.DecimalFormat;

import static org.junit.Assert.assertEquals;

public class WeightedSlidingWindowTest extends SparkTestCase {
    @Test
    public void shouldGiveAverageAccordingToWeights() {
        Weights weights = new Weights(3);
        weights.add(0.2);
        weights.add(0.3);
        weights.add(0.5);

        WeightedSlidingWindow slidingWindow = new WeightedSlidingWindow(3, weights);
        slidingWindow.add(3);
        slidingWindow.add(4);
        slidingWindow.add(5);

        Double expected = 4.3;
        Double actual = Double.parseDouble(new DecimalFormat("##.##").format(slidingWindow.average()));
        assertEquals(expected, actual);

        slidingWindow.add(3);
        Double expected1 = 5.2;
        Double actual1 = Double.parseDouble(new DecimalFormat("##.##").format(slidingWindow.average()));
        assertEquals(expected1, actual1);
    }
}