package org.apache.prepbuddy.datacleansers.imputation;

import org.apache.prepbuddy.datacleansers.RowPurger;
import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.utils.RowRecord;

public class NaiveBayesSubstitution implements ImputationStrategy {

    private NaiveBayesClassifier naiveBayesClassifier;

    public NaiveBayesSubstitution(int... columnIndexes) {
        naiveBayesClassifier = new NaiveBayesClassifier(columnIndexes);
    }

    @Override
    public void prepareSubstitute(TransformableRDD rdd, final int columnIndex) {
        TransformableRDD trainingSet = rdd.removeRows(new RowPurger.Predicate() {
            @Override
            public Boolean evaluate(RowRecord record) {
                return record.valueAt(columnIndex).trim().isEmpty();
            }
        });
        naiveBayesClassifier.train(trainingSet, columnIndex);
    }


    @Override
    public String handleMissingData(RowRecord record) {
        return naiveBayesClassifier.makeDecision(record);
    }

}
