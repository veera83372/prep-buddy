package org.apache.prepbuddy.typesystem;

import java.util.ArrayList;
import java.util.List;

public enum BaseDataType {

    NUMERIC(DataType.INTEGER, DataType.DECIMAL),STRING(DataType.CURRENCY, DataType.EMAIL, DataType.URL);

    protected DataType[] subtypes;
    private static final String PATTERN = "\\d+(\\.\\d+|\\d+)|\\.\\d+";
    BaseDataType(DataType... subtypes) {
        this.subtypes = subtypes;
    }

    public static BaseDataType getBaseType(List<String> samples) {
        if (matchesWith(PATTERN,samples))
            return NUMERIC;
        return STRING;
    }

    private static boolean matchesWith(String regex, List<String> samples) {
        int counter = 0;
        for (String string : samples)
            if (string.matches(regex)) counter++;
        return (counter > samples.size()/2);
    }

    public DataType actualType(List<String> sampleData) {
        for (DataType subtype : this.subtypes) {
            if(subtype.isOfType(sampleData))
                return subtype;
        }
        return DataType.ALPHANUMERIC_STRING;
    }
}
