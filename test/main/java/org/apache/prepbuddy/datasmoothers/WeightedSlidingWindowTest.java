package org.apache.prepbuddy.datasmoothers;

import org.apache.prepbuddy.SparkTestCase;
import org.junit.Test;

import java.text.DecimalFormat;

import static org.junit.Assert.assertEquals;

public class WeightedSlidingWindowTest extends SparkTestCase {
    @Test
    public void shouldGiveAverageAccordingToWeights() {
        WeightedSlidingWindow slidingWindow = new WeightedSlidingWindow(3);
        slidingWindow.add(3);
        slidingWindow.add(4);
        slidingWindow.add(5);

        Double expected = 4.33;
        Double actual = Double.parseDouble(new DecimalFormat("##.##").format(slidingWindow.average()));
        assertEquals(expected, actual);

        slidingWindow.add(3);
        Double expected1 = 3.83;
        Double actual1 = Double.parseDouble(new DecimalFormat("##.##").format(slidingWindow.average()));
        assertEquals(expected1, actual1);
    }
}