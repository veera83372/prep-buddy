package org.apache.prepbuddy.smoothers;

import org.apache.prepbuddy.exceptions.ApplicationException;
import org.apache.prepbuddy.exceptions.ErrorMessages;

import java.io.Serializable;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

/**
 * Contains weights in sequence for the weighted sliding window.
 */
public class Weights implements Serializable {
    private int limit;
    private List<Double> weights;

    public Weights(int limit) {
        this.limit = limit;
        weights = new ArrayList<>(this.limit);
    }

    public void add(double value) {
        if (size() == limit)
            throw new ApplicationException(ErrorMessages.SIZE_LIMIT_IS_EXCEEDED);
        if (size() == limit - 1 && sumWith(value) != 1.0)
            throw new ApplicationException(ErrorMessages.WEIGHTS_SUM_IS_NOT_EQUAL_TO_ONE);
        weights.add(value);
    }

    public double get(int index) {
        return weights.get(index);
    }

    private double sumWith(double value) {
        Double sum = 0.0;
        for (Double oneValue : weights) {
            sum += oneValue;
        }
        sum += value;
        return Double.parseDouble(new DecimalFormat("##.#").format(sum));
    }

    public int size() {
        return weights.size();
    }
}
