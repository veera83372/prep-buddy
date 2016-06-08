package org.apache.prepbuddy.utils;

import java.util.ArrayList;
import java.util.List;

public class NumberListClosure {
    private int size;
    private List<Double> values;

    public NumberListClosure(int size) {
        values = new ArrayList<>(size);
        this.size = size;
    }

    public void add(double value) {
        if (values.size() < size)
            values.add(value);
        else swapToPreviousThanAdd(value);
    }

    private void swapToPreviousThanAdd(Double value) {
        List<Double> newValues = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            Double oneValue = values.get(i);
            if (i != 0)
                newValues.add(oneValue);
        }
        newValues.add(value);
        values = newValues;
    }

    public boolean isFull() {
        return values.size() == size;
    }

    public Double average() {
        return Double.valueOf(sum() / size);
    }

    public int sum() {
        int sum = 0;
        for (Double oneValue : values) {
            sum += oneValue;
        }
        return sum;
    }

}
