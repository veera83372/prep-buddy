package org.apache.prepbuddy.normalizers;

import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.spark.api.java.JavaDoubleRDD;

public class ZScoreNormalization implements NormalizationStrategy {

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
