package org.apache.prepbuddy.typesystem;

import java.util.ArrayList;
import java.util.List;

public enum DataType{
    INTEGER {
        @Override
        public boolean isOfType(List<String> sampleData) {
            return matchesWith(this.INT_PATTERN,sampleData);
        }
    }, URL {
        @Override
        public boolean isOfType(List<String> sampleData) {
            return matchesWith(this.URL_PATTERN,sampleData);
        }
    }, EMAIL {
        @Override
        public boolean isOfType(List<String> sampleData) {
            return matchesWith(this.EMAIL_PATTERN,sampleData);
        }
    }, CURRENCY {
        @Override
        public boolean isOfType(List<String> sampleData) {
            return matchesWith(this.CURRENCY_PATTERN,sampleData);
        }
    }, ALPHANUMERIC_STRING {
        @Override
        public boolean isOfType(List<String> sampleData) {
            return false;
        }
    }, DECIMAL {
        @Override
        public boolean isOfType(List<String> sampleData) {
            return matchesWith(this.DECIMAL_PATTERN,sampleData);
        }
    };

    protected final String INT_PATTERN = "\\d+";
    protected final String URL_PATTERN = " ";
    protected final String EMAIL_PATTERN = "^[_A-Za-z0-9-]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";
    protected final String CURRENCY_PATTERN = " ";
    protected final String DECIMAL_PATTERN = "\\.\\d+|\\d+\\.\\d+";

    private static boolean matchesWith(String regex, List<String> samples) {
        int counter = 0;
        for (String string : samples)
            if (string.matches(regex)) counter++;
        return (counter > samples.size()/2);
    }

    public abstract boolean isOfType(List<String> sampleData);
}
