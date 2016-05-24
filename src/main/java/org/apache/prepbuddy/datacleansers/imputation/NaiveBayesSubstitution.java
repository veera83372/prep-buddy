package org.apache.prepbuddy.datacleansers.imputation;

import org.apache.prepbuddy.datacleansers.RowPurger;
import org.apache.prepbuddy.groupingops.TextFacets;
import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.utils.RowRecord;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class NaiveBayesSubstitution implements ImputationStrategy {
    private int[] columnIndexes;
    private List<List<Tuple2<String, Integer>>> groupedFacets;
    private long count;
    private Set<String> classKeys;

    public NaiveBayesSubstitution(int... columnIndexes) {
        this.columnIndexes = columnIndexes;
    }

    @Override
    public void prepareSubstitute(TransformableRDD rdd, final int columnIndex) {
        TransformableRDD trainingSet = rdd.removeRows(new RowPurger.Predicate() {
            @Override
            public Boolean evaluate(RowRecord record) {
                return record.valueAt(columnIndex).trim().isEmpty();
            }
        });
        setCount(trainingSet);
        setClassKeys(trainingSet, columnIndex);

        List<TextFacets> textFacets = listOfTextFacets(trainingSet, columnIndex);
        setGroupedFacets(textFacets);

    }

    private void setGroupedFacets(List<TextFacets> textFacets) {
        groupedFacets = new ArrayList<>();
        for (TextFacets textFacet : textFacets) {
            groupedFacets.add(textFacet.rdd().collect());
        }
    }

    private void setClassKeys(TransformableRDD trainingSet, int columnIndex) {
        classKeys = trainingSet.listFacets(columnIndex).rdd().collectAsMap().keySet();
    }

    private void setCount(TransformableRDD trainingSet) {
        count = trainingSet.count();
    }


    private List<TextFacets> listOfTextFacets(TransformableRDD rdd, int columnIndex) {
        List<TextFacets> facetsRddList = new ArrayList<>();
        for (int index : columnIndexes) {
            facetsRddList.add(rdd.listFacets(index, columnIndex));
        }
        return facetsRddList;
    }

    @Override
    public String handleMissingData(RowRecord record) {
        List<Double> probs = BayesianProbability(record);


        return null;
    }

    private List<Double> BayesianProbability(RowRecord record) {
        List<Double> probs = new ArrayList<>();
        for (String classKey : classKeys) {
            double probability = 0;
            for (int columnIndex : columnIndexes) {
                probability *=  conditionalProbability(classKey, record.valueAt(columnIndex), columnIndex);
            }
            probability *= classKeysProbability();
            probs.add(probability);
        }
        return probs;
    }

    private double conditionalProbability(String classKey, String secondValue, int columnIndex) {
        double intersectionCount = countOf(secondValue + " " +classKey, columnIndex);
        return intersectionCount / count;

    }

    private double countOf(String value, int columnIndex) {
        List<Tuple2<String, Integer>> listOfTuple = groupedFacets.get(columnIndex);
        for (Tuple2<String, Integer> tuple : listOfTuple) {
            if (tuple._1().equals(value))
                return tuple._2();
        }
        return 0;
    }

    private double classKeysProbability() {
        return 1 / classKeys.size();
    }
}
