package org.apache.prepbuddy.qualityanalyzers;

import java.io.Serializable;
import java.util.List;

/**
 * It tries to infer the data type of the column
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
