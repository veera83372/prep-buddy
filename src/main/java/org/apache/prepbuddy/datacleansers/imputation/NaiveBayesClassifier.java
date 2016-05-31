package org.apache.prepbuddy.datacleansers.imputation;

import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.utils.PivotTable;
import org.apache.prepbuddy.utils.RowRecord;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class NaiveBayesClassifier implements Serializable {
    private int[] independentColumnIndexes;
    private List<List<Tuple2<String, Integer>>> groupedFacets;
    private long count;
    private List<Tuple2<String, Integer>> rowKeys;
    private LikelihoodTable likelihoodTable;
    private PivotTable<Integer> pivotTable;

    public NaiveBayesClassifier(int... columnIndexes) {
        this.independentColumnIndexes = columnIndexes;
    }

    public void train(TransformableRDD rdd, final int missingColumnIndex) {
        rowKeys = rdd.listFacets(missingColumnIndex).rdd().collect();
        //for each column in rdd generate a pivot table of it against the missing data column based on count
        pivotTable = rdd.pivotByCounts(missingColumnIndex, independentColumnIndexes);

        setCount(rdd);
    }


    public String makeDecision(RowRecord record) {
        List<Tuple2<String, Probability>> probabilities = bayesianProbabilities(record);
        Tuple2<String, Probability> highest = probabilities.get(0);
        for (Tuple2<String, Probability> tuple : probabilities) {
            if (tuple._2().isGreaterThan(highest._2())) {
                highest = tuple;
            }
        }
        return highest._1();
    }


    private void setCount(TransformableRDD trainingSet) {
        count = trainingSet.count();
    }

    private List<Tuple2<String, Probability>> bayesianProbabilities(RowRecord record) {
        List<Tuple2<String, Probability>> probabilities = new ArrayList<>();

        for (Tuple2<String, Integer> categoricalKey : rowKeys) {
            Probability bayesianProbability = Probability.create(1);
            for (int columnIndex : independentColumnIndexes) {
                Probability bayesProbability = bayesProbability(record.valueAt(columnIndex), categoricalKey);
                bayesianProbability = bayesianProbability.multiply(bayesProbability);
            }
            Probability categoricalKeyProbability = Probability.create((double) categoricalKey._2() / count);
            bayesianProbability = bayesianProbability.multiply(categoricalKeyProbability);
            probabilities.add(new Tuple2<>(categoricalKey._1(), bayesianProbability));
        }
        return probabilities;
    }

    private Probability bayesProbability(String otherColumnValue, Tuple2<String, Integer> categoricalKey) {
        Integer intersectionCount = pivotTable.valueAt(categoricalKey._1(), otherColumnValue);
        double classKeyCount = categoricalKey._2();
        return Probability.create(intersectionCount / classKeyCount);
    }

}
