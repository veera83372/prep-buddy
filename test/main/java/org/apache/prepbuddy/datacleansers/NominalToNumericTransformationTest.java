package org.apache.prepbuddy.datacleansers;

import org.apache.prepbuddy.SparkTestCase;
import org.apache.prepbuddy.coreops.ColumnTransformation;
import org.apache.prepbuddy.coreops.DataTransformation;
import org.apache.prepbuddy.coreops.DatasetTransformations;
import org.apache.prepbuddy.filetypes.FileType;
import org.apache.prepbuddy.utils.DefaultValue;
import org.apache.prepbuddy.utils.Replacement;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.util.Arrays;

import static junit.framework.Assert.assertEquals;


public class NominalToNumericTransformationTest extends SparkTestCase {


    @Test
    public void shouldTransformABinaryNominalToNumeric() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("X,Y,", "A,B,Female"));

        DatasetTransformations datasetTransformations = new DatasetTransformations();
        ColumnTransformation columnTransformation = new ColumnTransformation(2);

        columnTransformation.setupImputation(new Imputation() {
            @Override
            protected String handleMissingData(String[] record) {
                return "Male";
            }
        });
        columnTransformation.setupNominalToNumeric(new DefaultValue(1),
                new Replacement<String, Integer>("Male", 0),
                new Replacement<String, Integer>("Female", 1));


        datasetTransformations.addColumnTransformations(columnTransformation);


        DataTransformation transformation = new DataTransformation();
        JavaRDD<String> transformed = transformation.apply(initialDataset, datasetTransformations, FileType.CSV);
        String expected = "X,Y,0";
        String actual = transformed.first();
        assertEquals(expected, actual);
    }
}