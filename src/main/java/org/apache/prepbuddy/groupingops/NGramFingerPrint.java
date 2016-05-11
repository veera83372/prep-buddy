package org.apache.prepbuddy.groupingops;

import scala.Tuple2;

import java.util.List;

public class NGramFingerPrint implements Algorithm{

    private final int nGram;

    public NGramFingerPrint(int nGram) {

        this.nGram = nGram;
    }

    @Override
    public Clusters getClusters(List<Tuple2<String, Integer>> tuples) {
        Clusters clusters = new Clusters();
        for (Tuple2<String, Integer> tuple : tuples) {
            String key = FingerprintingAlgorithms.generateNGramFingerprint(tuple._1(), nGram);
            clusters.add(key, tuple);
        }
        return clusters;
    }
}
