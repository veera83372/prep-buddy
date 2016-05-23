package org.apache.prepbuddy.typesystem;

import java.io.Serializable;
import java.util.List;

public enum BaseDataType implements Serializable {

    NUMERIC(DataType.ZIP_CODE_US,
            DataType.MOBILE_NUMBER,
            DataType.IP_ADDRESS,
            DataType.LATITUDE,
            DataType.LONGITUDE,
            DataType.INTEGER,
            DataType.DECIMAL
    ),
    STRING(DataType.CURRENCY,
            DataType.EMAIL,
            DataType.URL,
            DataType.SOCIAL_SECURITY_NUMBER,
            DataType.ZIP_CODE_US,
            DataType.COUNTRY_CODE_2_CHARACTER,
            DataType.COUNTRY_CODE_3_CHARACTER,
            DataType.COUNTRY_NAME,
            DataType.TIMESTAMP
    );

    private final static String PATTERN = "^([+-]?\\d+?\\s?)(\\d*(\\.\\d+)?)+$";
    private final DataType[] subtypes;

    BaseDataType(DataType... subtypes) {
        this.subtypes = subtypes;
    }

    public static BaseDataType getBaseType(List<String> samples) {
        if (matchesWith(PATTERN, samples))
            return NUMERIC;
        return STRING;
    }

    private static boolean matchesWith(String regex, List<String> samples) {
        int counter = 0;
        int threshold = samples.size() / 2;
        for (String string : samples)
            if (string.matches(regex)) counter++;
        return (counter >= threshold);
    }

    public DataType actualType(List<String> sampleData) {
        for (DataType subtype : this.subtypes) if (subtype.isOfType(sampleData)) return subtype;
        return DataType.ALPHANUMERIC_STRING;
    }
}
