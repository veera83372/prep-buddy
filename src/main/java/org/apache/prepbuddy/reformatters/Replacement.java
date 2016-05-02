package org.apache.prepbuddy.reformatters;

import java.io.Serializable;

public class Replacement<O, N> implements Serializable{
    private O oldValue;
    private N newValue;

    public Replacement(O oldValue, N newValue) {
        this.oldValue = oldValue;
        this.newValue = newValue;
    }

    public String replacementValue() {
        return newValue.toString();
    }

    public boolean matches(O existing) {
        return existing != null && existing.equals(oldValue);
    }
}
