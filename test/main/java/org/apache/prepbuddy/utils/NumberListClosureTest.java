package org.apache.prepbuddy.utils;

import org.apache.prepbuddy.SparkTestCase;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class NumberListClosureTest extends SparkTestCase {
    @Test
    public void shouldAddValueTillFullThanSwapTheValues() {
        NumberListClosure numberListClosure = new NumberListClosure(3);
        numberListClosure.add(3);
        numberListClosure.add(3);
        numberListClosure.add(3);

        assertEquals(3.0, numberListClosure.average());

        numberListClosure.add(9);
        assertEquals(5.0, numberListClosure.average());

        numberListClosure.add(15);
        assertEquals(9.0, numberListClosure.average());
    }
}