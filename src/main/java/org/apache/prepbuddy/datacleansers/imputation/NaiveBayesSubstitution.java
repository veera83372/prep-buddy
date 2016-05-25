package org.apache.prepbuddy.datacleansers.imputation;

import org.apache.prepbuddy.datacleansers.RowPurger;
import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.utils.RowRecord;

public class NaiveBayesSubstitution implements ImputationStrategy {

    private NaiveBayesClassifier naiveBayesClassifier;
    private int[] independentColumnIndexes;

    public NaiveBayesSubstitution(int... independentColumnIndexes) {
        this.independentColumnIndexes = independentColumnIndexes;
        naiveBayesClassifier = new NaiveBayesClassifier(independentColumnIndexes);
    }

    @Override
    public void prepareSubstitute(TransformableRDD rdd, final int categoricalColumnIndex) {
        TransformableRDD trainingSet = rdd.removeRows(new RowPurger.Predicate() {
            @Override
            public Boolean evaluate(RowRecord record) {
                return record.valueAt(categoricalColumnIndex).trim().isEmpty();
            }
        });

        naiveBayesClassifier.train(trainingSet, categoricalColumnIndex);
    }


    @Override
    public String handleMissingData(RowRecord record) {
        return naiveBayesClassifier.makeDecision(record);
    }

}
