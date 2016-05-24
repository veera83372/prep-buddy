package org.apache.prepbuddy.datacleansers.imputation;

import org.apache.prepbuddy.exceptions.ApplicationException;
import org.apache.prepbuddy.exceptions.ErrorMessages;

import java.io.Serializable;
import java.text.DecimalFormat;

public class Probability implements Serializable {
    private double probability;

    private Probability(double probability) {
        this.probability = probability;
    }

    public static Probability create(double probability) {
        if (probability < 0 || probability > 1)
            throw new ApplicationException(ErrorMessages.PROBABILITY_IS_NOT_IN_RANGE);
        probability = Double.parseDouble(new DecimalFormat("##.####").format(probability));
        return new Probability(probability);
    }

    public Probability multiply(Probability otherProbability) {
        return Probability.create(otherProbability.probability * probability);
    }

    public boolean isGreaterThan(Probability other) {
        return probability > other.probability;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        Probability that = (Probability) o;

        return Double.compare(that.probability, probability) == 0;

    }

}
