package org.apache.prepbuddy.transformations.imputation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;

public class ImputationTest implements Serializable{
    private static SparkConf sparkConf;
    private static JavaSparkContext ctx;
    private static ImputationTransformation imputation;

    @Before
    public void setUp() throws Exception {
        sparkConf = new SparkConf().setAppName("Sample");
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

}
