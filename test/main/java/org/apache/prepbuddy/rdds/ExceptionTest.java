package org.apache.prepbuddy.rdds;

import org.apache.prepbuddy.SparkTestCase;
import org.apache.prepbuddy.exceptions.ApplicationException;
import org.apache.prepbuddy.groupingops.TextFacets;
import org.apache.prepbuddy.utils.Replacement;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.Arrays;

public class ExceptionTest extends SparkTestCase {
    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void replaceShouldThrowExceptionForInvalidColumnIndex() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12342"
        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);
        exception.expect(ApplicationException.class);
        initialRDD.replace(4, new Replacement("", ""));
    }

    @Test
    public void listFacetsShouldThrowExceptionForInvalidColumnIndex() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12342"
        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);
        exception.expect(ApplicationException.class);
        initialRDD.listFacets(4);
    }


    @Test
    public void listFacetsShouldThrowExceptionForNullColumnIndex() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12342"
        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataset);
        exception.expect(ApplicationException.class);
        TextFacets textFacets = initialRDD.listFacets(5);
    }

}
