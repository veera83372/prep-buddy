package org.apache.prepbuddy.datacleansers.imputation;

import org.apache.prepbuddy.groupingops.TextFacets;
import org.apache.prepbuddy.rdds.TransformableRDD;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class BayesProbabilityCalculator {
    private int[] columnIndexes;
    private List<List<Tuple2<String, Integer>>> allColumnFacets;
    private List<List<Tuple2<String, Integer>>> groupedFacets;
    private long count;
    private List<Tuple2<String, Integer>> categoricalKeys;

    public BayesProbabilityCalculator(int[] columnIndexes) {
        this.columnIndexes = columnIndexes;
    }

    public void prepare(TransformableRDD trainingSet, int columnIndex) {
        setCount(trainingSet);
        setGroupedFacets(trainingSet, columnIndex);
        setEachColumnsFacet(trainingSet, columnIndex);
    }

    private void setEachColumnsFacet(TransformableRDD trainingSet, int categoricalIndex) {
        allColumnFacets = new ArrayList<>();
        for (int columnIndex : columnIndexes) {
            allColumnFacets.add(trainingSet.listFacets(columnIndex).rdd().collect());
        }
        List<Tuple2<String, Integer>> categoricalKeys = trainingSet.listFacets(categoricalIndex).rdd().collect();
        setCategoricalKeys(categoricalKeys);
        allColumnFacets.add(categoricalKeys);
    }

    private void setCategoricalKeys(List<Tuple2<String, Integer>> categoricalKeys) {
        this.categoricalKeys = categoricalKeys;
    }

    private void setGroupedFacets(TransformableRDD rdd, int columnIndex) {
        List<TextFacets> facetsRddList = new ArrayList<>();
        for (int index : columnIndexes) {
            facetsRddList.add(rdd.listFacets(index, columnIndex));
        }

        groupedFacets = new ArrayList<>();
        for (TextFacets textFacet : facetsRddList) {
            groupedFacets.add(textFacet.rdd().collect());
        }
    }

    private void setCount(TransformableRDD rdd) {
        count = rdd.count();
    }

    private void setCategoricalKeys(TransformableRDD trainingSet, int columnIndex) {

    }
}
