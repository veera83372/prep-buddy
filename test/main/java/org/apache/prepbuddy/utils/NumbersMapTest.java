package org.apache.prepbuddy.utils;

import org.apache.prepbuddy.SparkTestCase;
import org.junit.Assert;
import org.junit.Test;

public class NumbersMapTest extends SparkTestCase {
    @Test
    public void shouldGiveKeyOfHighestValue() {
        NumbersMap map = new NumbersMap();
        map.put("one", 1);
        map.put("two", 2);
        map.put("four", 4);
        map.put("eight", 8);

        String expected = "eight";
        String actual = map.keyWithHighestValue();
        Assert.assertEquals(expected, actual);
    }
}