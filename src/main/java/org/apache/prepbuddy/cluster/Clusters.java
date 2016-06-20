package org.apache.prepbuddy.cluster;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Clusters contains all cluster of a column.
 */
public class Clusters {
    private List<Cluster> clusters = new ArrayList<>();

    public void add(String key, Tuple2<String, Integer> recordTuple) {
        Cluster cluster = getClusterOf(key);
        if (cluster == null) {
            cluster = new Cluster(key);
            clusters.add(cluster);
        }

        cluster.add(recordTuple);
    }

    private Cluster getClusterOf(String key) {
        for (Cluster cluster : clusters) {
            if (cluster.isOfKey(key))
                return cluster;
        }
        return null;
    }

    public List<Cluster> getAllClusters() {
        return clusters;
    }

    public List<Cluster> getClustersExactlyOfSize(int size) {
        List<Cluster> list = new ArrayList<>();
        for (Cluster cluster : clusters) {
            if (cluster.size() == size) {
                list.add(cluster);
            }
        }
        return list;
    }

    public List<Cluster> getClustersWithSizeGreaterThan(int threshold) {
        List<Cluster> list = new ArrayList<>();
        for (Cluster cluster : clusters) {
            if (cluster.size() > threshold) {
                list.add(cluster);
            }
        }
        return list;
    }
}
