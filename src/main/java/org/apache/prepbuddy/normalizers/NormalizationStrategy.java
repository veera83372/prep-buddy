package org.apache.prepbuddy.normalizers;

import org.apache.prepbuddy.rdds.TransformableRDD;

import java.io.Serializable;

public interface NormalizationStrategy extends Serializable {

    void prepare(TransformableRDD transformableRDD, int columnIndex);

    public String normalize(String rawValue);
}
