package org.apache.prepbuddy.datacleansers.imputation;

import org.apache.prepbuddy.groupingops.TextFacets;
import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.utils.*;
import scala.Tuple2;

import java.io.Serializable;
import java.util.List;

public class NaiveBayesClassifier implements Serializable {
    private PivotTable<Probability> probs;
    private int missingDataColumn;
    private int[] otherColumns;
    private List<String> permissibleValues;

    public NaiveBayesClassifier(int... columnIndexes) {
        this.otherColumns = columnIndexes;
    }

    public NaiveBayesClassifier(int missingDataColumn, int[] otherColumns) {
        this.missingDataColumn = missingDataColumn;
        this.otherColumns = otherColumns;
    }

    public void train(TransformableRDD trainingSet) {
        TextFacets facets = trainingSet.listFacets(missingDataColumn);
        permissibleValues = facets.cardinalValues();
        List<Tuple2<String, Integer>> rowKeys = facets.rdd().collect();


        PivotTable<Integer> frequencyTable = trainingSet.pivotByCount(missingDataColumn, otherColumns);
        final long totalRows = trainingSet.count();
        probs = (PivotTable<Probability>) frequencyTable.transform(new TransformationFunction<Integer, Probability>() {
            public Probability transform(Integer input) {
                double value = (double) input.intValue() / totalRows;
                Probability probability = new Probability(value);
                return probability;
            }

            @Override
            public Probability defaultValue() {
                return new Probability(0);
            }
        });

        for (Tuple2<String, Integer> rowKey : rowKeys) {
            Probability probability = new Probability((double) rowKey._2() / totalRows);
            probs.addEntry(rowKey._1(), rowKey._1(), probability);
        }
    }


    public String classify(RowRecord record) {
        NumbersMap numbersMap = new NumbersMap();
        for (String permissibleValue : permissibleValues) {
            ProductOfNumbers productOfNumbers = new ProductOfNumbers(1);
            Double permissibleProbabilityValue = probs.valueAt(permissibleValue, permissibleValue).value();
            for (int column : otherColumns) {
                String columnValue = record.valueAt(column);
                Probability probability = probs.valueAt(permissibleValue, columnValue);
                productOfNumbers.multiply(probability.value() / permissibleProbabilityValue);
            }
            productOfNumbers.multiply(permissibleProbabilityValue);
            numbersMap.put(permissibleValue, productOfNumbers.result());
        }
        return numbersMap.keyWithHighestValue();
    }


}
