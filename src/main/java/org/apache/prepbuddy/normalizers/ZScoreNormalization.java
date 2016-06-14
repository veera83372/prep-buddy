package org.apache.prepbuddy.normalizers;

import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.DoubleFunction;

/**
 * A normalizer technique which normalizes data by their standard score.
 * Formula for Z Score Normalization : (X - Mean) / Standard Deviation.
 */
public class ZScoreNormalization implements NormalizationStrategy {

    private Double standardDeviation;
    private Double mean;

    @Override
    public void prepare(TransformableRDD transformableRDD, int columnIndex) {
        JavaRDD<String> columnValue = transformableRDD.select(columnIndex);
        JavaDoubleRDD doubleRDD = columnValue.mapToDouble(new DoubleFunction<String>() {
            @Override
            public double call(String element) throws Exception {
                return Double.parseDouble(element);
            }
        });
        standardDeviation = doubleRDD.stdev();
        mean = doubleRDD.mean();
    }

    @Override
    public String normalize(String rawValue) {
        return String.valueOf((Double.valueOf(rawValue) - mean) / standardDeviation);
    }
}
