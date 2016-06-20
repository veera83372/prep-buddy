package org.apache.prepbuddy.cluster;

import scala.Tuple2;

import java.util.List;

/**
 * ClusteringAlgorithm is for implementing the algorithm which can be use to
 * clustering the column value
 */
public interface ClusteringAlgorithm {
    Clusters getClusters(List<Tuple2<String, Integer>> tuples);
}
