package org.apache.prepbuddy.utils;

import java.util.HashMap;
import java.util.Map;

public class NumbersMap {
    private final Map<String, Double> numbers = new HashMap<>();

    public void put(String key, double aNumber) {
        numbers.put(key, aNumber);
    }

    public String keyWithHighestValue() {
        String highestValueKey = "";
        Double highestValue = 0.0;
        for (String key : numbers.keySet()) {
            Double value = numbers.get(key);
            if (value > highestValue) {
                highestValue = value;
                highestValueKey = key;
            }
        }
        return highestValueKey;
    }
}
