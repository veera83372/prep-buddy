package org.apache.prepbuddy.facets;

import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class Clusters {
    private List<Cluster> clusters = new ArrayList<>();

    public void add(String key, Tuple2<String, Integer> recordTuple) {
        Cluster cluster = getClusterOf(key);
        if (cluster == null){
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

    public List<Cluster> getClusters() {
        return clusters;
    }
}
