package org.apache.prepbuddy.utils;

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

    public boolean doesNotContain(double aValue) {
        return aValue >= lowerBound && aValue <= uppperBound;
    }
}
