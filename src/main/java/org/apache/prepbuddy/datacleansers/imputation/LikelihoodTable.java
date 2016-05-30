package org.apache.prepbuddy.datacleansers.imputation;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LikelihoodTable implements Serializable {


    private final HashMap<String, Map<String, Probability>> lookupTable;

    public LikelihoodTable() {
        lookupTable = new HashMap<>();
    }

    public Probability lookup(String rowKey, String columnKey) {
        return lookupTable.get(rowKey).get(columnKey);
    }

    public void addRowKeys(List<String> values) {
        for (String value : values) {
            lookupTable.put(value, new HashMap<String, Probability>());
        }
    }

    public void setProbability(String rowKey, String columnKey, Probability probability) {
        lookupTable.get(rowKey).put(columnKey, probability);
    }

}

