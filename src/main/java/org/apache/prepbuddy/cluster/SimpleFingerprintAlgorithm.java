package org.apache.prepbuddy.cluster;

import org.apache.commons.lang.StringUtils;
import scala.Tuple2;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.TreeSet;

/**
 * This algorithm generates a key using Simple Fingerprint Algorithm for
 * every cardinal value (facet) in column and add them to the Cluster.
 */
public class SimpleFingerprintAlgorithm extends FingerprintAlgorithm {
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

        StringBuilder buffer = new StringBuilder();
        Iterator<String> iterator = set.iterator();

        while (iterator.hasNext()) {
            buffer.append(iterator.next());
            if (iterator.hasNext())
                buffer.append(' ');
        }
        return buffer.toString();
    }

    @Override
    public Clusters getClusters(List<Tuple2<String, Integer>> tuples) {
        Clusters clusters = new Clusters();
        for (Tuple2<String, Integer> tuple : tuples) {
            String key = generateSimpleFingerprint(tuple._1());
            clusters.add(key, tuple);
        }
        return clusters;
    }
}
