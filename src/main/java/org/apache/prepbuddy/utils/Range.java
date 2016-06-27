package org.apache.prepbuddy.utils;

import java.util.ArrayList;
import java.util.List;

public class Range {

    private final double lowerBound;
    private final double uppperBound;

    public Range() {
        this(0, 0);
    }

    public Range(double lowerBound, double uppperBound) {
        this.lowerBound = lowerBound;
        this.uppperBound = uppperBound;
    }

    public boolean contains(double aValue) {
        return aValue >= lowerBound && aValue <= uppperBound;
    }

    public List<Integer> lowerToUpperValues() {
        List<Integer> continuousValues = new ArrayList<>();
        for (int value = (int) lowerBound; value <= uppperBound; value++)
            continuousValues.add(value);
        return continuousValues;
    }
}
