package org.apache.prepbuddy.smoothingops;

import org.apache.prepbuddy.SparkTestCase;
import org.apache.prepbuddy.exceptions.ApplicationException;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class WeightsTest extends SparkTestCase {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void shouldThrowExceptionWhileAddingValueMoreThanSize() {
        Weights weights = new Weights(2);
        weights.add(0.2);
        weights.add(0.8);

        exception.expect(ApplicationException.class);
        weights.add(2);
    }

    @Test
    public void shouldTrowExceptionWhileGettingAnyWeightAndSumIsNotEqualToOne() {
        Weights weights = new Weights(2);
        weights.add(0.2);

        exception.expect(ApplicationException.class);
        weights.add(0.9);
    }

    @Test
    public void shouldTrowExceptionIfSumIsNotEqualToOne() {
        Weights weights = new Weights(2);
        weights.add(0.333);
        weights.add(0.666);

        Weights weight1 = new Weights(2);
        weight1.add(0.777);
        exception.expect(ApplicationException.class);
        weight1.add(0.3);
    }
}