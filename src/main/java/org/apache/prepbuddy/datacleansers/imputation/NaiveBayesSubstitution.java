package org.apache.prepbuddy.datacleansers.imputation;

import org.apache.prepbuddy.datacleansers.RowPurger;
import org.apache.prepbuddy.groupingops.TextFacets;
import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.utils.RowRecord;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

public class NaiveBayesSubstitution implements ImputationStrategy {
    private int[] columnIndexes;
    private List<List<Tuple2<String, Integer>>> groupedFacets;
    private long count;
    private List<Tuple2<String, Integer>> categoricalKeys;


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

        setCategoricalKeys(trainingSet, columnIndex);
        setGroupedFacets(trainingSet, columnIndex);
        setCount(trainingSet);
    }



    @Override
    public String handleMissingData(RowRecord record) {
        List<Probability> probabilities = bayesianProbabilities(record);
        Probability highest = Probability.create(0);
        for (Probability eachProbability : probabilities) {
//            eachProbability.show();
            if (eachProbability.isGreaterThan(highest)) {
                highest = eachProbability;
            }
        }
        return categoricalKeys.get(probabilities.indexOf(highest))._1();
    }

    private void setCategoricalKeys(TransformableRDD trainingSet, int columnIndex) {
        categoricalKeys = trainingSet.listFacets(columnIndex).rdd().collect();
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

    private void setCount(TransformableRDD trainingSet) {
        count = trainingSet.count();
    }

    private List<Probability> bayesianProbabilities(RowRecord record) {
        List<Probability> probabilities = new ArrayList<>();
        for (Tuple2<String, Integer> categoricalKey : categoricalKeys) {
            Probability bayesianProbability = Probability.create(1);

            for (int columnIndex : columnIndexes) {
                Probability bayesProbability = bayesProbability(record.valueAt(columnIndex), categoricalKey);
                bayesianProbability = bayesianProbability.multiply(bayesProbability);
            }
            Probability categoricalKeyProbability = Probability.create((double) categoricalKey._2() / count);
            bayesianProbability = bayesianProbability.multiply(categoricalKeyProbability);
            probabilities.add(bayesianProbability);
        }
        return probabilities;
    }

    private Probability bayesProbability(String otherColumnValue, Tuple2<String, Integer> categoricalKey) {
        String groupValue = otherColumnValue + " " + categoricalKey._1();
        double intersectionCount = countOf(groupValue);
        double classKeyCount = categoricalKey._2();
        return Probability.create(intersectionCount / classKeyCount);
    }

    private double countOf(String value) {
        for (List<Tuple2<String, Integer>> groupedFacet : groupedFacets) {
            for (Tuple2<String, Integer> tuple : groupedFacet) {
                if (tuple._1().trim().equals(value.trim())) {
                    return tuple._2();
                }
            }
        }
        return 0;
    }
}
