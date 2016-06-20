package org.apache.prepbuddy.cluster;

import java.util.regex.Pattern;

public abstract class FingerprintAlgorithm implements ClusteringAlgorithm {
    private static final Pattern PUNCTUATION_MATCHER = Pattern.compile("\\p{Punct}|[\\x00-\\x08\\x0A-\\x1F\\x7F]");

    static String removeAllPunctuations(String someString) {
        return PUNCTUATION_MATCHER.matcher(someString).replaceAll("");
    }
}
