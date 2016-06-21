package org.apache.prepbuddy.normalizers;

import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.spark.api.java.JavaDoubleRDD;

/**
 * A normalizer technique which normalizes data by their standard score.
 * Formula for Z Score Normalization : (X - Mean) / Standard Deviation.
 */
public class ZScoreNormalizer implements NormalizationStrategy {

    private Double standardDeviation;
    private Double mean;

    @Override
    public void prepare(TransformableRDD transformableRDD, int columnIndex) {
        JavaDoubleRDD doubleRDD = transformableRDD.toDoubleRDD(columnIndex);
        standardDeviation = doubleRDD.stdev();
        mean = doubleRDD.mean();
    }

    @Override
    public String normalize(String rawValue) {
        return String.valueOf((Double.valueOf(rawValue) - mean) / standardDeviation);
    }
}
