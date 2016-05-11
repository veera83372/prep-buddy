package org.apache.prepbuddy.groupingops;

import scala.Tuple2;

import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

public class NGramFingerprint extends FingerprintAlgorithm {

    private final int nGram;

    public NGramFingerprint(int nGram) {
        this.nGram = nGram;
    }

    public String generateNGramFingerprint(String someString) {
        someString = someString.trim();
        someString = someString.toLowerCase();
        someString = removeAllPunctuations(someString);

        TreeSet<String> set = getNGramSetOf(someString, nGram);
        StringBuilder buffer = new StringBuilder();
        Iterator<String> iterator = set.iterator();

        while (iterator.hasNext()) {
            buffer.append(iterator.next());
        }

        return buffer.toString();
    }

    private static TreeSet<String> getNGramSetOf(String someString, int nGram) {
        TreeSet<String> set = new TreeSet<>();
        char[] chars = someString.toCharArray();
        for (int i = 0; i + nGram <= chars.length; i++) {
            set.add(new String(chars, i, nGram));
        }
        return set;
    }

    @Override
    public Clusters getClusters(List<Tuple2<String, Integer>> tuples) {
        Clusters clusters = new Clusters();
        for (Tuple2<String, Integer> tuple : tuples) {
            String key = generateNGramFingerprint(tuple._1());
            clusters.add(key, tuple);
        }
        return clusters;
    }
}
