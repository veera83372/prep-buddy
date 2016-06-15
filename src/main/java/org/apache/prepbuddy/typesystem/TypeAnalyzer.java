package org.apache.prepbuddy.typesystem;

import java.io.Serializable;
import java.util.List;

/**
 * It analyses the type of sample data
 */
public class TypeAnalyzer implements Serializable {
    private List<String> sampleData;

    public TypeAnalyzer(List<String> sampleData) {
        this.sampleData = sampleData;
    }

    public DataType getType() {
        BaseDataType type = BaseDataType.getBaseType(sampleData);
        return type.actualType(sampleData);
    }
}
