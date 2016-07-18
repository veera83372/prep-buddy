package org.apache.datacommons.prepbuddy.api.java;

import org.apache.datacommons.prepbuddy.api.JavaSparkTestCase;
import org.apache.datacommons.prepbuddy.api.java.types.FileType;
import org.apache.datacommons.prepbuddy.utils.RowRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JavaTransformableRDDTest extends JavaSparkTestCase {

    @Test
    public void shouldBeAbleToDeduplicate() {
        JavaRDD<String> numbers = javaSparkContext.parallelize(Arrays.asList(
                "One,Two,Three",
                "One,Two,Three",
                "One,Two,Three",
                "Ten,Eleven,Twelve"
        ));
        JavaTransformableRDD javaTransformableRDD = new JavaTransformableRDD(numbers, FileType.CSV);
        JavaTransformableRDD javaRdd = javaTransformableRDD.deduplicate();
        assertEquals(2, javaRdd.count());
    }

    @Test
    public void shouldBeAbleToRemoveRowsAccordingToPredicate() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("X,Y,", "X,Y,", "XX,YY,ZZ"));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);

        JavaTransformableRDD purged = initialRDD.removeRows(new RowPurger() {
            @Override
            public Boolean evaluate(RowRecord record) {
                return record.valueAt(1).equals("YY");
            }
        });
        assertEquals(2, purged.count());
    }

    @Test
    public void shouldBeAbleToDeduplicateRecordsByConsideringTheGivenColumnsAsPrimaryKey() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12342"
        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);
        List<Integer> primaryKeyColumns = Arrays.asList(0, 3);
        JavaTransformableRDD deduplicatedRDD = initialRDD.deduplicate(primaryKeyColumns);
        assertEquals(3, deduplicatedRDD.count());
    }

    @Test
    public void shouldBeAbleToDetectDeduplicateRecordsByConsideringTheGivenColumnsAsPrimaryKey() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12342"
        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);
        JavaTransformableRDD duplicates = initialRDD.duplicates(Arrays.asList(0, 3));
        assertEquals(2, duplicates.count());

        List<String> listOfDuplicates = duplicates.collect();
        assertTrue(listOfDuplicates.contains("John,Male,USA,12343"));
        assertTrue(listOfDuplicates.contains("John,Male,India,12343"));
    }

    @Test
    public void shouldBeAbleToDetectDeduplicateRecordsByConsideringAllTheColumns() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12345"
        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset);
        JavaTransformableRDD duplicates = initialRDD.duplicates();
        assertEquals(1, duplicates.count());
    }

    @Test
    public void shouldBeAbleToSelectAColumnFromTheDataSet() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12342"
        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);
        JavaRDD<String> columnValues = initialRDD.select(0);
        List<String> expectedValues = Arrays.asList("Smith", "John", "John", "Smith");
        assertEquals(expectedValues, columnValues.collect());
    }

    @Test
    public void shouldBeAbleToSelectSomeFeaturesFromTheDataSet() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12342"
        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);

        JavaRDD<String> selectedFeatures = initialRDD.select(0, 1);
        List<String> expected = Arrays.asList("Smith,Male", "John,Male", "John,Male", "Smith,Male");
        assertEquals(expected, selectedFeatures.collect());
    }
}
