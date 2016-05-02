package org.apache.prepbuddy.reformatters;

import org.apache.prepbuddy.DatasetTransformations;
import org.apache.prepbuddy.preprocessor.FileType;
import org.apache.prepbuddy.transformations.DataTransformation;
import org.apache.prepbuddy.transformations.SparkTestCase;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.util.Arrays;

import static junit.framework.Assert.assertEquals;


public class NominalToNumericTransformationTest extends SparkTestCase {


    @Test
    public void shouldTransformABinaryNominalToNumeric() {
        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList("X,Y,Male", "A,B,Female"));


        DatasetTransformations datasetTransformations = new DatasetTransformations();
        ColumnTransformations columnTransformations = new ColumnTransformations(2);
        columnTransformations.setupNominalToNumeric(new DefaultValue(1),
                new Replacement<String, Integer>("Male", 0),
                new Replacement<String, Integer>("Female", 1));


        datasetTransformations.addColumnTransformations(columnTransformations);


        DataTransformation transformation = new DataTransformation();
        JavaRDD<String> transformed = transformation.apply(initialDataset, datasetTransformations, FileType.CSV);
        String expected = "X,Y,0";
        String actual = transformed.first();
        assertEquals(expected, actual);
    }
}