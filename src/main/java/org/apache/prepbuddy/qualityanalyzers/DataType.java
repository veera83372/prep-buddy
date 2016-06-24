package org.apache.prepbuddy.qualityanalyzers;

import org.apache.prepbuddy.utils.Range;

import java.io.Serializable;
import java.util.*;

/**
 * Data types that can be inferred by inspecting the data in a column
 * <p/>
 * todo :
 * Add the following new types
 * BOOLEAN
 * LATITUDE_LONGITUDE_PAIR
 * CATEGORICAL_INTEGER
 * CATEGORICAL_STRING
 * CREDIT_CARD
 * GENDER
 * HTTP_CODE
 * US_STATE
 * and user defined data types
 */
public enum DataType implements Serializable {
    EMPTY {
        @Override
        public boolean isOfType(List<String> sampleData) {
            List<String> emptyRepresentation = Arrays.asList("N\\A", "\\N", "NULL", "blank", "(blank)", "NaN");
            for (String dataValue : sampleData) {
                if (!emptyRepresentation.contains(dataValue))
                    return false;
            }
            return true;
        }
    },
    INTEGER {
        @Override
        public boolean isOfType(List<String> sampleData) {
            final String EXPRESSION = "^[+-]?\\d+$";
            boolean matches = matchesWith(EXPRESSION, sampleData);
            return matches && extraChecksAreSuccessful(sampleData);
        }

        private boolean extraChecksAreSuccessful(List<String> sampleData) {
            for (String dataPoint : sampleData) {
                if (dataPoint.contains(".")) return false;
            }
            return true;
        }
    },
    URL {
        @Override
        public boolean isOfType(List<String> sampleData) {
            final String EXPRESSION = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]$";
            return matchesWith(EXPRESSION, sampleData);
        }
    },
    EMAIL {
        @Override
        public boolean isOfType(List<String> sampleData) {
            final String EXPRESSION = "^[_A-Za-z0-9-]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";
            return matchesWith(EXPRESSION, sampleData);
        }
    },
    CURRENCY {
        @Override
        public boolean isOfType(List<String> sampleData) {
            final String EXPRESSION = "^(\\p{Sc})(\\d+|\\d+.\\d+)$";
            return matchesWith(EXPRESSION, sampleData);
        }
    },
    DECIMAL {
        @Override
        public boolean isOfType(List<String> sampleData) {
            final String EXPRESSION = "^[+-]?(\\.\\d+|\\d+\\.\\d+)$";
            return matchesWith(EXPRESSION, sampleData);
        }
    },
    SOCIAL_SECURITY_NUMBER {
        @Override
        public boolean isOfType(List<String> sampleData) {
            final String EXPRESSION = "^(\\d{3}-\\d{2}-\\d{4})$";
            return matchesWith(EXPRESSION, sampleData);
        }
    },
    IP_ADDRESS {
        @Override
        public boolean isOfType(List<String> sampleData) {
            final String EXPRESSION = "^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";
            return matchesWith(EXPRESSION, sampleData);
        }
    },
    ZIP_CODE_US {
        @Override
        public boolean isOfType(List<String> sampleData) {
            final String EXPRESSION = "^[0-9]{5}(?:-[0-9]{4})?$";
            return matchesWith(EXPRESSION, sampleData);
        }
    },
    ALPHANUMERIC_STRING {
        @Override
        public boolean isOfType(List<String> sampleData) {
            return false;
        }
    },
    COUNTRY_CODE_2_CHARACTER {
        @Override
        public boolean isOfType(List<String> sampleData) {
            TreeSet<String> char2countryCodes = new TreeSet<>(Arrays.asList(Locale.getISOCountries()));
            return matchInDictionary(sampleData, char2countryCodes);
        }
    },
    COUNTRY_CODE_3_CHARACTER {
        @Override
        public boolean isOfType(List<String> sampleData) {
            Set<String> countryCodes = getISO3Codes();
            return matchInDictionary(sampleData, countryCodes);
        }

        private Set<String> getISO3Codes() {
            String[] isoCountries = Locale.getISOCountries();
            TreeSet<String> countryCodes = new TreeSet<>();
            for (String country : isoCountries) {
                Locale locale = new Locale("", country);
                countryCodes.add(locale.getISO3Country());
            }
            return countryCodes;
        }
    },
    COUNTRY_NAME {
        @Override
        public boolean isOfType(List<String> sampleData) {
            Set<String> countryNames = getCountryNames();
            return matchInDictionary(sampleData, countryNames);
        }

        private Set<String> getCountryNames() {
            String[] isoCountries = Locale.getISOCountries();
            TreeSet<String> countryCodes = new TreeSet<>();
            for (String country : isoCountries) {
                Locale locale = new Locale("", country);
                countryCodes.add(locale.getDisplayCountry());
            }
            return countryCodes;
        }
    }, MOBILE_NUMBER {
        @Override
        public boolean isOfType(List<String> sampleData) {
            final String EXPRESSION = "^(([+]\\d+\\s)|0)?\\d{10}$";
            return matchesWith(EXPRESSION, sampleData);
        }
    }, TIMESTAMP {
        @Override
        public boolean isOfType(List<String> sampleData) {
            final String EXPRESSION = "(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}x?)";
            return matchesWith(EXPRESSION, sampleData);
        }
    }, LATITUDE {
        @Override
        public boolean isOfType(List<String> sampleData) {
            final String EXPRESSION = "^[-+]?([1-8]?\\d(\\.\\d+)?|90(\\.0+)?)$";
            boolean matched = matchesWith(EXPRESSION, sampleData);
            return matched && extraChecksAreSuccessful(sampleData);
        }

        private boolean extraChecksAreSuccessful(List<String> sampleData) {
            Range range = new Range(-90, 90);
            for (String dataPoint : sampleData) {
                if (!range.contains(Double.parseDouble(dataPoint))) return false;
            }
            return true;
        }
    }, LONGITUDE {
        @Override
        public boolean isOfType(List<String> sampleData) {
            final String EXPRESSION = "^[-+]?(180(\\.0+)?|((1[0-7]\\d)|([1-9]?\\d))(\\.\\d+)?)$";
            boolean matched = matchesWith(EXPRESSION, sampleData);
            return matched && extraChecksAreSuccessful(sampleData);
        }

        private boolean extraChecksAreSuccessful(List<String> sampleData) {
            Range range = new Range(-180, 180);
            for (String dataPoint : sampleData) {
                if (!range.contains(Double.parseDouble(dataPoint))) return false;
            }
            return true;
        }
    }, CATEGORICAL_INTEGER {
        @Override
        public boolean isOfType(List<String> sampleData) {
            int categoricalSize = 2;
            return isCategorical(sampleData,categoricalSize);
        }
    }, CATEGORICAL_STRING {
        @Override
        public boolean isOfType(List<String> sampleData) {
            int categoricalSize = 3;
            return isCategorical(sampleData,categoricalSize);
        }
    };

    protected boolean isCategorical(List<String> sampleData,int categoricalSize) {
        TreeSet<String> tree = new TreeSet<>();
        for (String element : sampleData)
            tree.add(element);
        return tree.size()<=categoricalSize;
    }

    protected boolean matchInDictionary(List<String> sampleData, Set<String> dictionary) {
        double qualifyingLimit = sampleData.size() * 0.75;
        Set<String> lowerCaseDictionary = new TreeSet<>();
        for (String element : dictionary)
            lowerCaseDictionary.add(element.toLowerCase());

        int matchCount = 0;
        for (String element : sampleData)
            if (lowerCaseDictionary.contains(element.toLowerCase()))
                matchCount++;
        return matchCount >= qualifyingLimit;
    }

    public boolean matchesWith(String regex, List<String> samples) {
        int counter = 0;
        int threshold = samples.size() / 2;
        for (String string : samples)
            if (string.matches(regex)) counter++;
        return (counter >= threshold);
    }

    public abstract boolean isOfType(List<String> sampleData);
}
