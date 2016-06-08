package org.apache.prepbuddy.utils;

import org.apache.prepbuddy.SparkTestCase;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class SlidingWindowTest extends SparkTestCase {
    @Test
    public void shouldAddValueTillFullThanSwapTheValues() {
        SlidingWindow slidingWindow = new SlidingWindow(3);
        slidingWindow.add(3);
        slidingWindow.add(3);
        slidingWindow.add(3);

        assertEquals(3.0, slidingWindow.average());

        slidingWindow.add(9);
        assertEquals(5.0, slidingWindow.average());

        slidingWindow.add(15);
        assertEquals(9.0, slidingWindow.average());
    }
}