package org.apache.prepbuddy.utils;

import java.io.Serializable;

public class Pair<O, N> implements Serializable {
    protected O firstValue;
    protected N secondValue;

    public Pair(O firstValue, N secondValue) {
        this.firstValue = firstValue;
        this.secondValue = secondValue;
    }
}
