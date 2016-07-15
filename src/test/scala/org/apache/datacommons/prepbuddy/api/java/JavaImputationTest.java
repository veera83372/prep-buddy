package org.apache.datacommons.prepbuddy.api.java;

import org.apache.datacommons.prepbuddy.api.JavaSparkTestCase;
import org.apache.datacommons.prepbuddy.api.java.types.FileType;
import org.apache.datacommons.prepbuddy.imputations.ApproxMeanSubstitution;
import org.apache.datacommons.prepbuddy.imputations.MeanSubstitution;
import org.apache.datacommons.prepbuddy.imputations.ModeSubstitution;
import org.apache.datacommons.prepbuddy.imputations.UnivariateLinearRegressionSubstitution;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JavaImputationTest extends JavaSparkTestCase {
    @Test
    public void shouldImputeTheValueWithTheMean() {
        JavaRDD<String> initialDataSet = javaSparkContext.parallelize(Arrays.asList(
                "07434677419,07371326239,Incoming,31,Wed Sep 15 19:17:44 +0100 2010",
                "07641036117,01666472054,Outgoing,20,Mon Feb 11 07:18:23 +0000 1980",
                "07641036117,07371326239,Incoming, ,Mon Feb 11 07:45:42 +0000 1980",
                "07641036117,07681546436,Missed,12,Mon Feb 11 08:04:42 +0000 1980"

        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataSet, FileType.CSV);
        JavaTransformableRDD imputed = initialRDD.impute(3, new MeanSubstitution());
        List<String> listOfRecord = imputed.collect();
        assertEquals(4, listOfRecord.size());
        String expected = "07641036117,07371326239,Incoming,21.0,Mon Feb 11 07:45:42 +0000 1980";
        assertTrue(listOfRecord.contains(expected));
    }

    @Test
    public void shouldImputeTheValueWithTheMeanApprox() {
        JavaRDD<String> initialDataSet = javaSparkContext.parallelize(Arrays.asList(
                "07434677419,07371326239,Incoming,31,Wed Sep 15 19:17:44 +0100 2010",
                "07641036117,01666472054,Outgoing,20,Mon Feb 11 07:18:23 +0000 1980",
                "07641036117,07371326239,Incoming, ,Mon Feb 11 07:45:42 +0000 1980",
                "07641036117,07681546436,Missed,12,Mon Feb 11 08:04:42 +0000 1980"

        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataSet, FileType.CSV);
        JavaTransformableRDD imputed = initialRDD.impute(3, new ApproxMeanSubstitution());
        List<String> listOfRecord = imputed.collect();
        String expected = "07641036117,07371326239,Incoming,21.0,Mon Feb 11 07:45:42 +0000 1980";
        assertTrue(listOfRecord.contains(expected));
    }

    @Test
    public void shouldImputeTheValueWithTheMostOccurredValue() throws Exception {
        JavaRDD<String> initialDataSet = javaSparkContext.parallelize(Arrays.asList(
                "07434677419,07371326239,Incoming,31,Wed Sep 15 19:17:44 +0100 2010",
                "07641036117,01666472054,Outgoing,31,Mon Feb 11 07:18:23 +0000 1980",
                "07641036117,07371326239,Incoming, ,Mon Feb 11 07:45:42 +0000 1980",
                "07641036117,07681546436,Missed,12,Mon Feb 11 08:04:42 +0000 1980"

        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataSet, FileType.CSV);
        JavaTransformableRDD imputed = initialRDD.impute(3, new ModeSubstitution());
        List<String> listOfRecord = imputed.collect();
        String expected = "07641036117,07371326239,Incoming,31,Mon Feb 11 07:45:42 +0000 1980";
        assertTrue(listOfRecord.contains(expected));
    }

    @Test
    public void shouldImputeTheValueWithTheRegression() throws Exception {
        JavaRDD<String> initialDataSet = javaSparkContext.parallelize(Arrays.asList(
                "60,3.1", "61,3.6", "62,3.8", "63,4", "65,4.1", "64,"
        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataSet, FileType.CSV);
        JavaTransformableRDD imputed = initialRDD.impute(1, new UnivariateLinearRegressionSubstitution(0));
        List<String> listOfRecord = imputed.collect();

        String expected = "64,4.06";
        assertTrue(listOfRecord.contains(expected));
    }

    @Test
    public void shouldImputeTheValueWithTheRegressionTest2() throws Exception {
        JavaRDD<String> initialDataSet = javaSparkContext.parallelize(Arrays.asList(
                "3.4,5.67", "3.9,4.81", "2.6,4.93", "1.9,6.21", "2.2,6.83", "3.3,5.61", "1.7,5.45", "2.4,4.94", "2.8,5.73",
                "3.6,"
        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataSet, FileType.CSV);
        JavaTransformableRDD imputed = initialRDD.impute(1, new UnivariateLinearRegressionSubstitution(0));
        List<String> listOfRecord = imputed.collect();

        String expected = "3.6,5.24";
        assertTrue(listOfRecord.contains(expected));
    }
}
