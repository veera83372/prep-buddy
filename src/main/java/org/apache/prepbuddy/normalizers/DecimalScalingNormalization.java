package org.apache.prepbuddy.normalizers;

import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.spark.api.java.JavaDoubleRDD;

public class DecimalScalingNormalization implements NormalizationStrategy {

    private int length;

    @Override
    public void prepare(TransformableRDD transformableRDD, int columnIndex) {
        JavaDoubleRDD doubleRDD = transformableRDD.toDoubleRDD(columnIndex);
        length = String.valueOf(doubleRDD.max().intValue()).length();
    }

    @Override
    public String normalize(String rawValue) {
        return String.valueOf(Double.parseDouble(rawValue) / Math.pow(10, length - 1));
    }
}
