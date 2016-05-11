package org.apache.prepbuddy.groupingops;

import org.apache.commons.lang.StringUtils;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class LevenshteinDistance implements ClusteringAlgorithm {
    @Override
    public Clusters getClusters(List<Tuple2<String, Integer>> tuples) {
        Clusters clusters = new Clusters();

        List<Integer> indexes = new ArrayList<>();
        for (int i = 0; i < tuples.size(); i++) {
            Tuple2<String, Integer> tuple = tuples.get(i);
            String tupleKey = tuple._1();
            clusters.add(tupleKey, tuple);
            for (int j = i + 1; j < tuples.size(); j++) {
                Tuple2<String, Integer> otherTuple = tuples.get(j);
                String otherTupleKey = otherTuple._1();
                int distance = StringUtils.getLevenshteinDistance(tupleKey, otherTupleKey);
                if (distance < 4 && !(indexes.contains(j))) {
                    clusters.add(tupleKey, otherTuple);
                    indexes.add(j);
                }
            }
        }
        return clusters;
    }
}
