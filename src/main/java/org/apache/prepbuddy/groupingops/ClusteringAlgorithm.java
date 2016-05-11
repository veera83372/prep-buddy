package org.apache.prepbuddy.groupingops;

import scala.Tuple2;

import java.util.List;

public interface ClusteringAlgorithm {
    Clusters getClusters(List<Tuple2<String, Integer>> tuples);
}
