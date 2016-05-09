package org.apache.prepbuddy.utils;

public class Replacement<O, N> extends Pair<O,N> {

    public Replacement(O oldValue, N newValue) {
        super(oldValue, newValue);
    }

    public String replacementValue() {
        return secondValue.toString();
    }

    public boolean matches(O existing) {
        return existing != null && existing.equals(firstValue);
    }
}
