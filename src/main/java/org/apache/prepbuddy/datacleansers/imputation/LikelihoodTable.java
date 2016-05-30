package org.apache.prepbuddy.datacleansers.imputation;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LikelihoodTable {


    private final HashMap<String, Map<String, Double>> lookupTable;

    public LikelihoodTable() {
        lookupTable = new HashMap<>();
    }

    public double lookup(String rowKey, String columnKey) {
        return lookupTable.get(rowKey).get(columnKey);
    }

    public void addRowKeys(List<String> values) {
        for (String value : values) {
            lookupTable.put(value, new HashMap<String, Double>());
        }
    }

    public void setProbability(String rowKey, String columnKey, double probability) {
        lookupTable.get(rowKey).put(columnKey, probability);
    }

}

