package org.apache.prepbuddy.normalizers;

import org.apache.prepbuddy.rdds.TransformableRDD;

public interface NormalizationStrategy {

    void prepare(TransformableRDD transformableRDD, int columnIndex);

    String normalize(String rawValue);
}
