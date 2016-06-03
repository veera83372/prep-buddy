package org.apache.prepbuddy.datacleansers.imputation;

import org.apache.prepbuddy.SparkTestCase;
import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.utils.RowRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.util.Arrays;

import static junit.framework.Assert.assertEquals;

public class UnivariateLinearRegressionSubstitutionTest extends SparkTestCase {
    @Test
    public void shouldAbleToPredictMissingFieldValue() {
        JavaRDD<String> initialDataSet = javaSparkContext.parallelize(Arrays.asList(
                "60,3.1", "61,3.6", "62,3.8", "63,4", "65,4.1"
        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataSet);
        UnivariateLinearRegressionSubstitution strategy = new UnivariateLinearRegressionSubstitution(0);
        strategy.prepareSubstitute(initialRDD, 1);

        String[] record = new String[]{"64"};
        String expected = "4.06";
        assertEquals(expected, strategy.handleMissingData(new RowRecord(record)));
    }

    @Test
    public void shouldPredictMissingValue() {
        JavaRDD<String> initialDataSet = javaSparkContext.parallelize(Arrays.asList(
                "3.4,5.67", "3.9 ,4.81", "2.6 ,4.93", "1.9, 6.21", "2.2,6.83", "3.3,5.61", "1.7,5.45", "2.4,4.94", "2.8,5.73"
        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataSet);
        UnivariateLinearRegressionSubstitution strategy = new UnivariateLinearRegressionSubstitution(0);
        strategy.prepareSubstitute(initialRDD, 1);

        String[] record = new String[]{"3.6"};
        String expected = "5.24";
        assertEquals(expected, strategy.handleMissingData(new RowRecord(record)));
    }
}