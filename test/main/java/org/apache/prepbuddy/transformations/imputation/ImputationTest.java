package org.apache.prepbuddy.transformations.imputation;

import org.apache.commons.io.FileUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.Serializable;
import java.util.Arrays;

public class ImputationTest implements Serializable{
    private static SparkConf sparkConf;
    private static JavaSparkContext ctx;
    private static ImputationTransformation imputation;

    @Before
    public void setUp() throws Exception {
        FileUtils.deleteDirectory(new File("data/transformed"));
        sparkConf = new SparkConf().setAppName("Test").setMaster("local");
        ctx = new JavaSparkContext(sparkConf);
        imputation = new ImputationTransformation();
    }

    @Test
    public void shouldCallbackTheMissingDataHandler() {
        JavaRDD<String> initialDataset = ctx.parallelize(Arrays.asList(",,4,5"));
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
        JavaRDD<String> initialDataset = ctx.parallelize(Arrays.asList("1,,4,5","3,5,6"));
        Remover remover = new Remover();
        remover.onColumn(0);
        remover.onColumn(1);
        JavaRDD<String> transformed = imputation.removeIfNull(initialDataset, remover);
        transformed.saveAsTextFile("data/transformed");
    }

    @After
    public void tearDown() throws Exception {
        ctx.stop();
    }
}
