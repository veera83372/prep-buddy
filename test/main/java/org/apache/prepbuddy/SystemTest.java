package org.apache.prepbuddy;

import org.apache.prepbuddy.preprocessor.FileType;
import org.apache.prepbuddy.reformatters.*;
import org.apache.prepbuddy.transformations.DataTransformation;
import org.apache.prepbuddy.transformations.SparkTestCase;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.util.Arrays;

import static junit.framework.Assert.assertEquals;

public class SystemTest extends SparkTestCase {

    @Test
    public void shouldExecuteASeriesOfTransformsOnADataset() {
        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList("X,Y,", "X,Y,", "XX,YY,ZZ"));

        DatasetTransformations datasetTransformations = new DatasetTransformations();
        datasetTransformations.deduplicateRows();

        datasetTransformations.removeRows(new RowPredicate() {
            @Override
            public Boolean evaluate(String record) {
                return record.split(",")[1].equals("YY");
            }
        });

//        datasetTransformations.appendRows("new file", "existing file");

        ColumnTransformations columnTransformations = new ColumnTransformations(2);

        columnTransformations.setupImputation(new ImputationTransformation() {
            @Override
            protected String handleMissingData(RowRecord record) {
                return "Male";
            }
        });
        columnTransformations.setupNominalToNumeric(new DefaultValue(1),
                new Replacement<>("Male", 0), new Replacement<>("Female", 1));


        datasetTransformations.addColumnTransformations(columnTransformations);


        DataTransformation transformation = new DataTransformation();
        JavaRDD<String> transformed = transformation.apply(initialDataset, datasetTransformations, FileType.CSV);
        String expected = "X,Y,0";
        assertEquals(1, transformed.count());
        String actual = transformed.first();
        assertEquals(expected, actual);

    }
}
