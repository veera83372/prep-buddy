package org.datacommons.prepbuddy.api.java;

import org.apache.spark.api.java.JavaRDD;
import org.datacommons.prepbuddy.api.JavaSparkTestCase;
import org.datacommons.prepbuddy.api.java.types.FileType;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertTrue;

public class JavaMergeSplitTest extends JavaSparkTestCase {

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

        List<String> result = transformableRDD.splitByFieldLength(3, Arrays.asList(3, 10), false).collect();

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

    @Test
    public void shouldSplitTheSpecifiedColumnValueByDelimiterWhileKeepingTheOriginalColumn() {
        List<String> data = Arrays.asList(
                "John,Male,21,+91-4382313832,Canada",
                "Smith, Male, 30,+01-5314343462, UK",
                "Larry, Male, 23,+00-9815432975, USA",
                "Fiona, Female,18,+89-1015709854,USA"
        );
        JavaRDD<String> dataset = javaSparkContext.parallelize(data);
        JavaTransformableRDD transformableRDD = new JavaTransformableRDD(dataset, FileType.CSV);

        List<String> result = transformableRDD.splitByDelimiter(3, "-", true).collect();
        assertTrue(result.size() == 4);
        assertTrue(result.contains("John,Male,21,+91-4382313832,Canada,+91,4382313832"));
        assertTrue(result.contains("Smith,Male,30,+01-5314343462,UK,+01,5314343462"));
    }

    @Test
    public void shouldSplitTheSpecifiedColumnValueByDelimiterWhileRemovingTheOriginalColumn() {
        List<String> data = Arrays.asList(
                "John,Male,21,+91-4382313832,Canada",
                "Smith, Male, 30,+01-5314343462, UK",
                "Larry, Male, 23,+00-9815432975, USA",
                "Fiona, Female,18,+89-1015709854,USA"
        );
        JavaRDD<String> dataset = javaSparkContext.parallelize(data);
        JavaTransformableRDD transformableRDD = new JavaTransformableRDD(dataset, FileType.CSV);

        List<String> result = transformableRDD.splitByDelimiter(3, "-", false).collect();
        assertTrue(result.size() == 4);
        assertTrue(result.contains("John,Male,21,Canada,+91,4382313832"));
        assertTrue(result.contains("Smith,Male,30,UK,+01,5314343462"));
    }

    @Test
    public void shouldSplitTheGivenColumnByDelimiterIntoGivenNumberOfSplit() {
        List<String> data = Arrays.asList(
                "John\tMale\t21\t+91-4382-313832\tCanada",
                "Smith\tMale\t30\t+01-5314-343462\tUK",
                "Larry\tMale\t23\t+00-9815-432975\tUSA",
                "Fiona\tFemale\t18\t+89-1015-709854\tUSA"
        );
        JavaRDD<String> dataset = javaSparkContext.parallelize(data);
        JavaTransformableRDD transformableRDD = new JavaTransformableRDD(dataset, FileType.TSV);

        List<String> result = transformableRDD.splitByDelimiter(3, "-", false, 2).collect();

        assertTrue(result.size() == 4);
        assertTrue(result.contains("John\tMale\t21\tCanada\t+91\t4382-313832"));
        assertTrue(result.contains("Smith\tMale\t30\tUK\t+01\t5314-343462"));
    }
}
