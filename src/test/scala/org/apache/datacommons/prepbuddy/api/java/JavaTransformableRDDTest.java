package org.apache.datacommons.prepbuddy.api.java;

import org.apache.datacommons.prepbuddy.api.JavaSparkTestCase;
import org.apache.datacommons.prepbuddy.api.java.types.FileType;
import org.apache.datacommons.prepbuddy.utils.PivotTable;
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

    @Test
    public void sizeShouldGiveTheNumberOfColumnInRdd() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList(
                "Smith,Male,USA,12345",
                "John,Male,USA,12343",
                "John,Male,India,12343",
                "Smith,Male,USA,12342"
        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);
        int size = initialRDD.numberOfColumns();
        assertEquals(4, size);
    }

    @Test
    public void pivotByCountsShouldGiveCountsWithGivenColumns() {
        JavaRDD<String> initialDataSet = javaSparkContext.parallelize(Arrays.asList(
                "known,new,long,home,skips",
                "unknown,new,short,work,reads",
                "unknown,follow Up,long,work,skips",
                "known,follow Up,long,home,skips",
                "known,new,short,home,reads",
                "known,follow Up,long,work,skips",
                "unknown,follow Up,short,work,skips",
                "unknown,new,short,work,reads",
                "known,follow Up,long,home,skips",
                "known,new,long,work,skips",
                "unknown,follow Up,short,home,skips",
                "known,new,long,work,skips",
                "known,follow Up,short,home,reads",
                "known,new,short,work,reads",
                "known,new,short,home,reads",
                "known,follow Up,short,work,reads",
                "known,new,short,home,reads",
                "unknown,new,short,work,reads"
        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataSet);
        PivotTable<Integer> pivotTable = initialRDD.pivotByCount(4, Arrays.asList(0, 1, 2, 3));
        int valueAtSkipsLong = pivotTable.valueAt("skips", "long");
        assertEquals(7, valueAtSkipsLong);

        int valueAtReadsLong = pivotTable.valueAt("reads", "long");
        assertEquals(0, valueAtReadsLong);
    }

    @Test
    public void shouldMergeGivenColumnsWithTheSeparator() {
        List<String> data = Arrays.asList(
                "John,Male,21,Canada",
                "Smith, Male, 30, UK",
                "Larry, Male, 23, USA",
                "Fiona, Female,18,USA"
        );
        JavaRDD<String> dataset = javaSparkContext.parallelize(data);
        JavaTransformableRDD transformableRDD = new JavaTransformableRDD(dataset, FileType.CSV);

        List<Integer> columnIndexes = Arrays.asList(0, 3, 1);
        List<String> result = transformableRDD.mergeColumns(columnIndexes, "_", false).collect();

        assertTrue(result.size() == 4);
        assertTrue(result.contains("21,John_Canada_Male"));
        assertTrue(result.contains("23,Larry_USA_Male"));
    }

    @Test
    public void shouldMergeColumnsBySpaceIfNoSeparatorIsGiven() {
        List<String> data = Arrays.asList(
                "John,Male,21,Canada",
                "Smith, Male, 30, UK",
                "Larry, Male, 23, USA",
                "Fiona, Female,18,USA"
        );
        JavaRDD<String> dataset = javaSparkContext.parallelize(data);
        JavaTransformableRDD transformableRDD = new JavaTransformableRDD(dataset, FileType.CSV);

        List<Integer> columnIndexes = Arrays.asList(0, 3, 1);
        List<String> result = transformableRDD.mergeColumns(columnIndexes).collect();

        assertTrue(result.size() == 4);
        assertTrue(result.contains("21,John Canada Male"));
        assertTrue(result.contains("23,Larry USA Male"));
    }

    @Test
    public void shouldMergeColumnsByKeepingTheOriginalColumns() {
        List<String> data = Arrays.asList(
                "John,Male,21,Canada",
                "Smith, Male, 30, UK",
                "Larry, Male, 23, USA",
                "Fiona, Female,18,USA"
        );
        JavaRDD<String> dataset = javaSparkContext.parallelize(data);
        JavaTransformableRDD transformableRDD = new JavaTransformableRDD(dataset, FileType.CSV);

        List<Integer> columnIndexes = Arrays.asList(0, 3, 1);
        List<String> result = transformableRDD.mergeColumns(columnIndexes, "_", true).collect();

        assertTrue(result.size() == 4);
        assertTrue(result.contains("John,Male,21,Canada,John_Canada_Male"));
        assertTrue(result.contains("Larry,Male,23,USA,Larry_USA_Male"));
    }

    @Test
    public void shouldSplitTheSpecifiedColumnValueAccordingToTheGivenLengthsByRemovingOriginalColumns() {
        List<String> data = Arrays.asList(
                "John,Male,21,+914382313832,Canada",
                "Smith, Male, 30,+015314343462, UK",
                "Larry, Male, 23,+009815432975, USA",
                "Fiona, Female,18,+891015709854,USA"
        );
        JavaRDD<String> dataset = javaSparkContext.parallelize(data);
        JavaTransformableRDD transformableRDD = new JavaTransformableRDD(dataset, FileType.CSV);

        List<String> result = transformableRDD.splitByFieldLength(3, Arrays.asList(3, 10)).collect();

        assertTrue(result.size() == 4);
        assertTrue(result.contains("John,Male,21,Canada,+91,4382313832"));
        assertTrue(result.contains("Smith,Male,30,UK,+01,5314343462"));

    }

    @Test
    public void shouldSplitTheSpecifiedColumnValueAccordingToTheGivenLengthsByKeepingOriginalColumns() {
        List<String> data = Arrays.asList(
                "John,Male,21,+914382313832,Canada",
                "Smith, Male, 30,+015314343462, UK",
                "Larry, Male, 23,+009815432975, USA",
                "Fiona, Female,18,+891015709854,USA"
        );
        JavaRDD<String> dataset = javaSparkContext.parallelize(data);
        JavaTransformableRDD transformableRDD = new JavaTransformableRDD(dataset, FileType.CSV);

        List<String> result = transformableRDD.splitByFieldLength(3, Arrays.asList(3, 10), true).collect();

        assertTrue(result.size() == 4);
        assertTrue(result.contains("John,Male,21,+914382313832,Canada,+91,4382313832"));
        assertTrue(result.contains("Smith,Male,30,+015314343462,UK,+01,5314343462"));
    }
}
