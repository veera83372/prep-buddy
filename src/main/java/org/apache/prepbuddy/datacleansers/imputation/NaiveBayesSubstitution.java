package org.apache.prepbuddy.datacleansers.imputation;

import org.apache.prepbuddy.datacleansers.RowPurger;
import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.utils.RowRecord;

/**
 * This imputation strategy is based on Naive Bayes Algorithm which
 * is the probabilistic classifier. And this implementation is only
 * for impute the categorical values.
 */
public class NaiveBayesSubstitution implements ImputationStrategy {

    private NaiveBayesClassifier naiveBayesClassifier;
    private int[] otherColumns;

    public NaiveBayesSubstitution(int... independentColumnIndexes) {
        otherColumns = independentColumnIndexes;
    }

    @Override
    public void prepareSubstitute(TransformableRDD rdd, final int missingDataColumn) {
        naiveBayesClassifier = new NaiveBayesClassifier(missingDataColumn, otherColumns);
        TransformableRDD trainingSet = rdd.removeRows(new RowPurger.Predicate() {
            @Override
            public Boolean evaluate(RowRecord record) {
                return record.hasEmptyColumn();

            }
        });

        naiveBayesClassifier.train(trainingSet);
    }


    @Override
    public String handleMissingData(RowRecord record) {
        return naiveBayesClassifier.classify(record);
    }

}
