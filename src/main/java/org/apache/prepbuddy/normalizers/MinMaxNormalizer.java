package org.apache.prepbuddy.normalizers;


import org.apache.prepbuddy.rdds.TransformableRDD;

import java.util.Comparator;

public class MinMaxNormalizer implements NormalizationStrategy {

    private Comparator<String> comparator;

    public MinMaxNormalizer(Comparator<String> comparator) {
        this.comparator = comparator;
    }

    @Override
    public void prepare(TransformableRDD transformableRDD, int columnIndex) {
        String min = transformableRDD.min(comparator);
        String max = transformableRDD.max(comparator);
    }

    @Override
    public String normalize(String rawValue) {
        return null;
    }
}
