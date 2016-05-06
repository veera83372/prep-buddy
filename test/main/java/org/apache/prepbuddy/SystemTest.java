package org.apache.prepbuddy;

import org.apache.prepbuddy.coreops.ColumnTransformation;
import org.apache.prepbuddy.coreops.DataTransformation;
import org.apache.prepbuddy.coreops.DatasetTransformations;
import org.apache.prepbuddy.datacleansers.Imputation;
import org.apache.prepbuddy.datacleansers.RowPurger;
import org.apache.prepbuddy.filetypes.FileType;
import org.apache.prepbuddy.utils.DefaultValue;
import org.apache.prepbuddy.utils.Replacement;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.util.Arrays;

import static junit.framework.Assert.assertEquals;

public class SystemTest extends SparkTestCase {

    @Test
    public void shouldExecuteASeriesOfTransformsOnADataset() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("X,Y,", "X,Y,", "XX,YY,ZZ"));

        DatasetTransformations datasetTransformations = new DatasetTransformations();
        datasetTransformations.deduplicateRows();

        datasetTransformations.removeRows(new RowPurger.Predicate() {
            @Override
            public Boolean evaluate(String record) {
                return record.split(",")[1].equals("YY");
            }
        });

//        datasetTransformations.appendRows("new file", "existing file");

        ColumnTransformation columnTransformation = new ColumnTransformation(2);

        columnTransformation.setupImputation(new Imputation() {
            @Override
            protected String handleMissingData(String[] record) {
                return "Male";
            }
        });
        columnTransformation.setupNominalToNumeric(new DefaultValue(1),
                new Replacement<>("Male", 0), new Replacement<>("Female", 1));


        datasetTransformations.addColumnTransformations(columnTransformation);


        DataTransformation transformation = new DataTransformation();
        JavaRDD<String> transformed = transformation.apply(initialDataset, datasetTransformations, FileType.CSV);
        String expected = "X,Y,0";
        assertEquals(1, transformed.count());
        String actual = transformed.first();
        assertEquals(expected, actual);
    }
}
