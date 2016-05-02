package org.apache.prepbuddy.reformatters;

import java.io.Serializable;

public class DefaultValue<T> implements Serializable{
    private T value;

    public DefaultValue(T value) {
        this.value = value;
    }

    public String asString() {
        return value.toString();
    }
}
