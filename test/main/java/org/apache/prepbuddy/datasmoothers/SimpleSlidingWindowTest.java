package org.apache.prepbuddy.datasmoothers;

import org.apache.prepbuddy.SparkTestCase;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class SimpleSlidingWindowTest extends SparkTestCase {
    @Test
    public void shouldAddValueTillFullThanSwapTheValues() {
        SimpleSlidingWindow slidingWindow = new SimpleSlidingWindow(3);
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