package org.apache.prepbuddy.typesystem;

import java.io.Serializable;
import java.util.*;

/**
 * Data types that can be inferred by the TransformableRDD.
 */
public enum DataType implements Serializable {
    INTEGER {
        @Override
        public boolean isOfType(List<String> sampleData) {
            final String INT_PATTERN = "^[+-]?\\d+$";
            return matchesWith(INT_PATTERN, sampleData);
        }
    },
    URL {
        @Override
        public boolean isOfType(List<String> sampleData) {
            final String URL_PATTERN = "^(https?|ftp|file)://[-a-zA-Z0-9+&@#/%?=~_|!:,.;]*[-a-zA-Z0-9+&@#/%=~_|]$";
            return matchesWith(URL_PATTERN, sampleData);
        }
    },
    EMAIL {
        @Override
        public boolean isOfType(List<String> sampleData) {
            final String EMAIL_PATTERN = "^[_A-Za-z0-9-]+(\\.[_A-Za-z0-9-]+)*@[A-Za-z0-9]+(\\.[A-Za-z0-9]+)*(\\.[A-Za-z]{2,})$";
            return matchesWith(EMAIL_PATTERN, sampleData);
        }
    },
    CURRENCY {
        @Override
        public boolean isOfType(List<String> sampleData) {
            final String CURRENCY_PATTERN = "^(\\p{Sc})(\\d+|\\d+.\\d+)$";
            return matchesWith(CURRENCY_PATTERN, sampleData);
        }
    },
    DECIMAL {
        @Override
        public boolean isOfType(List<String> sampleData) {
            final String DECIMAL_PATTERN = "^[+-]?(\\.\\d+|\\d+\\.\\d+)$";
            return matchesWith(DECIMAL_PATTERN, sampleData);
        }
    },
    SOCIAL_SECURITY_NUMBER {
        @Override
        public boolean isOfType(List<String> sampleData) {
            final String SSN_PATTERN = "^(\\d{3}-\\d{2}-\\d{4})$";
            return matchesWith(SSN_PATTERN, sampleData);
        }
    },
    IP_ADDRESS {
        @Override
        public boolean isOfType(List<String> sampleData) {
            final String IP_PATTERN = "^(([01]?\\d\\d?|2[0-4]\\d|25[0-5])\\.){3}([01]?\\d\\d?|2[0-4]\\d|25[0-5])$";
            return matchesWith(IP_PATTERN, sampleData);
        }
    },
    ZIP_CODE_US {
        @Override
        public boolean isOfType(List<String> sampleData) {
            final String ZIP_PATTERN = "^[0-9]{5}(?:-[0-9]{4})?$";
            return matchesWith(ZIP_PATTERN, sampleData);
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
            return predicate(sampleData, char2countryCodes);
        }
    },
    COUNTRY_CODE_3_CHARACTER {
        @Override
        public boolean isOfType(List<String> sampleData) {
            Set<String> countryCodes = getISO3Codes();
            return predicate(sampleData, countryCodes);
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
            return predicate(sampleData, countryNames);
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
            final String PHONE_PATTERN = "^(([+]\\d+\\s)|0)?\\d{10}$";
            return matchesWith(PHONE_PATTERN, sampleData);
        }
    }, TIMESTAMP {
        @Override
        public boolean isOfType(List<String> sampleData) {
            final String PATTERN = "(\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}\\.\\d{3}x?)";
            return matchesWith(PATTERN, sampleData);
        }
    }, LATITUDE {
        @Override
        public boolean isOfType(List<String> sampleData) {
            final String PATTERN = "^[-+]?([1-8]?\\d(\\.\\d+)?|90(\\.0+)?)$";
            return matchesWith(PATTERN, sampleData);
        }
    }, LONGITUDE {
        @Override
        public boolean isOfType(List<String> sampleData) {
            final String PATTERN = "^[-+]?(180(\\.0+)?|((1[0-7]\\d)|([1-9]?\\d))(\\.\\d+)?)$";
            return matchesWith(PATTERN, sampleData);
        }
    };

    protected boolean predicate(List<String> sampleData, Set<String> originalData) {
        int tolerance = sampleData.size() / 4;
        int threshold = originalData.size() + tolerance;
        Set<String> sample = new TreeSet<>();
        for (String element : sampleData) sample.add(element.toLowerCase());
        for (String element : originalData) sample.add(element.toLowerCase());
        return (sample.size() <= threshold);
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
