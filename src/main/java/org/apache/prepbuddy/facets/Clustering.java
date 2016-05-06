package org.apache.prepbuddy.facets;

import org.apache.prepbuddy.filetypes.FileType;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.List;

public class Clustering {
    public static Clusters byFingerPrint(JavaRDD<String> initialDataset, int columnIndex, FileType fileType) {
        Clusters clusters = new Clusters();
        FingerPrint fingerPrint = new FingerPrint();
        TextFacets textFacets = FacetUtils.listTextFacets(initialDataset, columnIndex, fileType);
        JavaPairRDD<String, Integer> rdd = textFacets.rdd();
        List<Tuple2<String, Integer>> tuples = rdd.collect();

        for (Tuple2<String, Integer> tuple : tuples) {
            String key = fingerPrint.key(tuple._1());
            clusters.add(key, tuple);
        }
        return clusters;
    }
}
