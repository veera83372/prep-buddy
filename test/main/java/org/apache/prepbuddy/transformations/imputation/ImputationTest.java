package org.apache.prepbuddy.transformations.imputation;

import org.apache.commons.io.FileUtils;
import org.apache.prepbuddy.transformations.SparkTestCase;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;

public class ImputationTest extends SparkTestCase {
    private ImputationTransformation imputation;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        FileUtils.deleteDirectory(new File("data/transformed"));
        imputation = new ImputationTransformation();
    }

    @Test
    public void shouldCallbackTheMissingDataHandler() {
        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList(",,4,5"));
        Imputers imputers = new Imputers();
        imputers.add(0, new Imputers.HandlerFunction() {
            @Override
            public Object handleMissingField(int columnIndex) {
                return "1234567890";
            }
        });
        imputers.add(1, new Imputers.HandlerFunction() {
            @Override
            public Object handleMissingField(int columnIndex) {
                return "000000";
            }
        });

        JavaRDD<String> transformed = imputation.handleMissingFields(initialDataset, imputers);
        transformed.saveAsTextFile("data/transformed");
    }

    @Test
    public void shouldRemoveTheEntireRowWhenDataIsMissing() {
        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList("1,,4,5", "3,5,6"));
        Remover remover = new Remover();
        remover.onColumn(0);
        remover.onColumn(1);
        JavaRDD<String> transformed = imputation.removeIfNull(initialDataset, remover);
        transformed.saveAsTextFile("data/transformed");
    }

}
