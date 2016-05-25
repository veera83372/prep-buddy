package org.apache.prepbuddy.normalizers;

import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.DoubleFunction;

public class DecimalScalingNormalization implements NormalizationStrategy {

    private int length;

    @Override
    public void prepare(TransformableRDD transformableRDD, int columnIndex) {
        JavaRDD<String> columnValues = transformableRDD.select(columnIndex);
        JavaDoubleRDD doubleRDD = columnValues.mapToDouble(new DoubleFunction<String>() {
            @Override
            public double call(String element) throws Exception {
                return Double.parseDouble(element);
            }
        });
        length = String.valueOf(doubleRDD.max().intValue()).length();
    }

    @Override
    public String normalize(String rawValue) {
        return String.valueOf(Double.parseDouble(rawValue) / Math.pow(10, length - 1));
    }
}
