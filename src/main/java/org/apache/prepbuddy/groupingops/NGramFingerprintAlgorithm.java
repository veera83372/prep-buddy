package org.apache.prepbuddy.groupingops;

import scala.Tuple2;

import java.util.List;
import java.util.TreeSet;

/**
 * This algorithm generates a key using N Gram Fingerprint Algorithm for
 * every cardinal value (facet) in column and add them to the Cluster.
 */
public class NGramFingerprintAlgorithm extends FingerprintAlgorithm {

    private final int nGram;

    public NGramFingerprintAlgorithm(int nGram) {
        this.nGram = nGram;
    }

    private static TreeSet<String> getNGramSetOf(String someString, int nGram) {
        TreeSet<String> set = new TreeSet<>();
        char[] chars = someString.toCharArray();
        for (int i = 0; i + nGram <= chars.length; i++) {
            set.add(new String(chars, i, nGram));
        }
        return set;
    }

    public String generateNGramFingerprint(String someString) {
        someString = someString.trim();
        someString = someString.toLowerCase();
        someString = removeAllPunctuations(someString);

        TreeSet<String> set = getNGramSetOf(someString, nGram);
        StringBuilder buffer = new StringBuilder();

        for (String aSet : set) {
            buffer.append(aSet);
        }

        return buffer.toString();
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
