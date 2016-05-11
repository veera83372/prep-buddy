package org.apache.prepbuddy.groupingops;

import scala.Tuple2;

import java.util.List;

import static org.apache.prepbuddy.groupingops.FingerprintingAlgorithms.generateSimpleFingerprint;

public class SimpleFingerPrint implements Algorithm {
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
