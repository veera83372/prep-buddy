package org.apache.prepbuddy.datacleansers.imputation;

import org.apache.prepbuddy.groupingops.TextFacets;
import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.utils.RowRecord;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class NaiveBayesClassifier implements Serializable {
    private int[] independentColumnIndexes;
    private List<List<Tuple2<String, Integer>>> groupedFacets;
    private long count;
    private List<Tuple2<String, Integer>> categoricalKeys;
    private LikelihoodTable likelihoodTable;
    private List<String> rowKeys;

    public NaiveBayesClassifier(int... columnIndexes) {
        this.independentColumnIndexes = columnIndexes;
    }

    public void train(TransformableRDD rdd, final int missingColumnIndex) {
        likelihoodTable = new LikelihoodTable();
        TextFacets facets = rdd.listFacets(missingColumnIndex);
        rowKeys = facets.cardinalValues();
        likelihoodTable.addRowKeys(rowKeys);

        //for each column in rdd generate a pivot table of it against the missing data column based on count
        rdd.pivotByCount(missingColumnIndex, 0);
        rdd.pivotByCounts(missingColumnIndex, independentColumnIndexes);


//        List<Tuple2<String, Integer>> tuples = facets.rdd().collect();


//        for (Tuple2<String, Integer> tuple : listOfTuple) {
//            String eachKey = tuple._1();
//            Probability probability = Probability.create(tuple._2() / count);
//            likelihoodTable.setProbability(eachKey, eachKey, probability);
//        }

        setProbability(rdd, missingColumnIndex);
        setCount(rdd);
    }


    public String makeDecision(RowRecord record) {
        List<Probability> probabilities = bayesianProbabilities(record);
        Probability highest = Probability.create(0);
        for (Probability eachProbability : probabilities) {
            if (eachProbability.isGreaterThan(highest)) {
                highest = eachProbability;
            }
        }
        int probableCategoryIndex = probabilities.indexOf(highest);
        return categoricalKeys.get(probableCategoryIndex)._1();
    }

    private void setProbability(TransformableRDD rdd, int columnIndex) {
        List<List<Tuple2<String, Integer>>> facetsRddList = new ArrayList<>();
        for (int index : independentColumnIndexes) {
            List<Tuple2<String, Integer>> groupedFacets = rdd.listFacets(new int[]{index, columnIndex}).rdd().collect();
            for (String rowKey : rowKeys) {
                for (Tuple2<String, Integer> groupedFacet : groupedFacets) {

                }
            }
        }

    }

    private void setCount(TransformableRDD trainingSet) {
        count = trainingSet.count();
    }

    private List<Probability> bayesianProbabilities(RowRecord record) {
        List<Probability> probabilities = new ArrayList<>();
        for (Tuple2<String, Integer> categoricalKey : categoricalKeys) {
            Probability bayesianProbability = Probability.create(1);

            for (int columnIndex : independentColumnIndexes) {
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
