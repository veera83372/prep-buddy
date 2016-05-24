package org.apache.prepbuddy.normalizers;


import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.typesystem.FileType;

import static java.lang.Double.parseDouble;
import static java.lang.Double.valueOf;


public class MinMaxNormalizer implements NormalizationStrategy {

    private SerializableComparator<String> serializableComparator;
    private Double min;
    private Double max;

    public MinMaxNormalizer(SerializableComparator<String> serializableComparator) {
        this.serializableComparator = serializableComparator;
    }

    @Override
    public void prepare(TransformableRDD transformableRDD, int columnIndex) {
        FileType fileType = transformableRDD.fileType;
        min = valueOf(fileType.parseRecord(transformableRDD.min(serializableComparator))[columnIndex]);
        max = valueOf(fileType.parseRecord(transformableRDD.max(serializableComparator))[columnIndex]);
    }

    @Override
    public String normalize(String rawValue) {
        return String.valueOf((parseDouble(rawValue) - min) / (max - min));
    }
}
