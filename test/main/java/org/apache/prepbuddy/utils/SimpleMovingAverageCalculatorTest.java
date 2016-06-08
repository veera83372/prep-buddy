package org.apache.prepbuddy.utils;

import org.apache.prepbuddy.SparkTestCase;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class SimpleMovingAverageCalculatorTest extends SparkTestCase {
    @Test
    public void shouldAddValueTillFullThanSwapTheValues() {
        SimpleMovingAverageCalculator simpleMovingAverageCalculator = new SimpleMovingAverageCalculator(3);
        simpleMovingAverageCalculator.add(3);
        simpleMovingAverageCalculator.add(3);
        simpleMovingAverageCalculator.add(3);

        assertEquals(3.0, simpleMovingAverageCalculator.average());

        simpleMovingAverageCalculator.add(9);
        assertEquals(5.0, simpleMovingAverageCalculator.average());

        simpleMovingAverageCalculator.add(15);
        assertEquals(9.0, simpleMovingAverageCalculator.average());
    }
}