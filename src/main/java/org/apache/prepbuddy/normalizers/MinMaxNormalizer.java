package org.apache.prepbuddy.normalizers;

import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.spark.api.java.JavaDoubleRDD;

import static java.lang.Double.parseDouble;

/**
 * A normalizer which scales the data within the specified range.
 */
public class MinMaxNormalizer implements NormalizationStrategy {
    private final int minRange;
    private final int maxRange;

    private Double minValue;
    private Double maxValue;

    public MinMaxNormalizer(int minRange, int maxRange) {
        this.minRange = minRange;
        this.maxRange = maxRange;
    }

    public MinMaxNormalizer() {
        this(0, 1);
    }

    @Override
    public void prepare(TransformableRDD transformableRDD, int columnIndex) {
        JavaDoubleRDD doubleRDD = transformableRDD.toDoubleRDD(columnIndex);
        maxValue = doubleRDD.max();
        minValue = doubleRDD.min();
    }

    @Override
    public String normalize(String rawValue) {
        double normalizedValue = ((parseDouble(rawValue) - minValue) / (maxValue - minValue)) * (maxRange - minRange) + minRange;
        return String.valueOf(normalizedValue);
    }
}
