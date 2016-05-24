package org.apache.prepbuddy.normalizers;

import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.DoubleFunction;

public class ZScoreNormalization implements NormalizationStrategy {

    private Double standardDeviation;
    private Double mean;

    @Override
    public void prepare(TransformableRDD transformableRDD, int columnIndex) {
        JavaRDD<String> columnValue = transformableRDD.select(columnIndex);
        JavaDoubleRDD doubleRDD = columnValue.mapToDouble(new DoubleFunction<String>() {
            @Override
            public double call(String s) throws Exception {
                return Double.parseDouble(s);
            }
        });
        standardDeviation = doubleRDD.stdev();
        mean = doubleRDD.mean();
        System.out.println("standardDeviation = " + standardDeviation);
        System.out.println("mean = " + mean);
    }

    @Override
    public String normalize(String rawValue) {
        return String.valueOf((Double.valueOf(rawValue) - mean) / standardDeviation);
    }
}
