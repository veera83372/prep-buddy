package org.apache.prepbuddy.transformations.Replacement;

import org.apache.prepbuddy.preprocessor.FileTypes;
import org.apache.prepbuddy.transformations.imputation.ImputationTransformation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class ReplacerTest implements Serializable {
    private static SparkConf sparkConf;
    private static JavaSparkContext ctx;
    private static ImputationTransformation imputation;

    @Before
    public void setUp() throws Exception {
        sparkConf = new SparkConf().setAppName("Test").setMaster("local");
        ctx = new JavaSparkContext(sparkConf);
        imputation = new ImputationTransformation();
    }
    @Test
    public void shouldCallbackTheMissingDataHandler() {
        JavaRDD<String> initialDataset = ctx.parallelize(Arrays.asList("1,2,3,4"));

        Replacer replacer = new Replacer();
        replacer.add(0, new Replacer.ReplaceHandler<String, String>() {
            @Override
            public String replace(String value) {
                return "10";
            }
        });

        ReplaceTransformation replaceTransformation = new ReplaceTransformation();
        JavaRDD<String> replacedDataset = replaceTransformation.replace(initialDataset, replacer, FileTypes.CSV);

        String expected = "10,2,3,4";
        String actual = replacedDataset.first();
        System.out.printf(String.valueOf(replacedDataset.collect()));
        assertEquals(expected, actual);
    }


    @After
    public void tearDown() throws Exception {
        ctx.stop();
    }
}