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
    private List<List<Tuple2<String, Integer>>> allColumnFacets;
    private long count;
    private List<String> classKeys;

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
        setAllColumnFacets(trainingSet);

    }

    private void setAllColumnFacets(TransformableRDD trainingSet) {
        allColumnFacets = new ArrayList<>();
        for (int columnIndex : columnIndexes) {
            allColumnFacets.add(trainingSet.listFacets(columnIndex).rdd().collect());
        }
    }

    private void setGroupedFacets(List<TextFacets> textFacets) {
        groupedFacets = new ArrayList<>();
        for (TextFacets textFacet : textFacets) {
            groupedFacets.add(textFacet.rdd().collect());
        }
    }

    private void setClassKeys(TransformableRDD trainingSet, int columnIndex) {
        List<Tuple2<String, Integer>> listOfClassTuple = trainingSet.listFacets(columnIndex).rdd().collect();
        classKeys = new ArrayList<>();
        for (Tuple2<String, Integer> tuple : listOfClassTuple) {
            classKeys.add(tuple._1());
        }
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
        double normalizingConstant = 0.0;
        for (Double prob : probs) {
            normalizingConstant *= 1 / prob;
        }
        Double highest = 0.0;
        double probability = 0;
        for (Double prob : probs) {
            Double multipliedWithConstant = prob * normalizingConstant;
            if (multipliedWithConstant > highest) {
                highest = multipliedWithConstant;
                probability = prob;
            }
        }
        return classKeys.get(probs.indexOf(probability));
    }

    private List<Double> BayesianProbability(RowRecord record) {
        List<Double> probs = new ArrayList<>();
        for (String classKey : classKeys) {
            double probability = 0;
            for (int columnIndex : columnIndexes) {
                probability *= conditionalProbability(classKey, record.valueAt(columnIndex));
            }
            probability *= classKeysProbability();
            probs.add(probability);
        }
        return probs;
    }

    private double conditionalProbability(String classKey, String secondValue) {
        double intersectionCount = countOf(secondValue + " " + classKey);
        double otherColumnCount = countOfInAllFcates(secondValue);
        return intersectionCount / otherColumnCount;

    }

    private double countOfInAllFcates(String value) {
        for (List<Tuple2<String, Integer>> allColumnFacet : allColumnFacets) {
            for (Tuple2<String, Integer> tuple : allColumnFacet) {
                if (tuple._1().equals(value)) {
                    return tuple._2();
                }
            }
        }
        return 0;
    }

    private double countOf(String value) {
        for (List<Tuple2<String, Integer>> groupedFacet : groupedFacets) {
            for (Tuple2<String, Integer> tuple : groupedFacet) {
                if (tuple._1().equals(value)) {
                    return tuple._2();
                }
            }
        }
        return 0;
    }

    private double classKeysProbability() {
        return 1 / classKeys.size();
    }
}
