package org.apache.prepbuddy.groupingops;

import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.TreeSet;
import java.util.regex.Pattern;

public class FingerprintingAlgorithms {

    private static final Pattern PUNCTUATION_MATCHER = Pattern.compile("\\p{Punct}|[\\x00-\\x08\\x0A-\\x1F\\x7F]");

    public static String generateSimpleFingerprint(String someString) {
        someString = someString.trim();
        someString = someString.toLowerCase();
        someString = removeAllPunctuations(someString);
        String[] fragments = StringUtils.split(someString);

        return rearrangeAlphabetically(fragments);
    }

    private static String rearrangeAlphabetically(String[] fragments) {
        TreeSet<String> set = new TreeSet<>();
        Collections.addAll(set, fragments);

        StringBuffer buffer = new StringBuffer();
        Iterator<String> iterator = set.iterator();

        while (iterator.hasNext()) {
            buffer.append(iterator.next());
            if (iterator.hasNext())
                buffer.append(' ');
        }
        return buffer.toString();
    }

    private static String removeAllPunctuations(String someString) {
        return PUNCTUATION_MATCHER.matcher(someString).replaceAll("");
    }

    public static String generateNGramFingerprint(String someString, int nGram) {
        someString = someString.trim();
        someString = someString.toLowerCase();
        someString = removeAllPunctuations(someString);

        TreeSet<String> set = getNGramSetOf(someString, nGram);
        StringBuffer buffer = new StringBuffer();
        Iterator<String> iterator = set.iterator();

        while (iterator.hasNext())
            buffer.append(iterator.next());

        return buffer.toString();
    }

    private static TreeSet<String> getNGramSetOf(String someString, int nGram) {
        TreeSet<String> set = new TreeSet<String>();
        char[] chars = someString.toCharArray();
        for (int i = 0; i + nGram <= chars.length; i++) {
            set.add(new String(chars, i, nGram));
        }
        return set;
    }
}
