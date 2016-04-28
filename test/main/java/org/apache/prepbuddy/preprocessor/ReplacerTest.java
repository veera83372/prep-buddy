package org.apache.prepbuddy.preprocessor;

import org.apache.log4j.Level;
import org.apache.prepbuddy.transformations.imputation.ImputationTransformation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;

import static org.apache.log4j.Logger.getLogger;
import static org.junit.Assert.assertEquals;

public class ReplacerTest implements Serializable {
    private static SparkConf sparkConf;
    private static JavaSparkContext ctx;
    private static ImputationTransformation imputation;
    private static ImputationTransformation imputationOfTSV;

    @Before
    public void setUp() throws Exception {
        sparkConf = new SparkConf().setAppName("Test").setMaster("local");
        ctx = new JavaSparkContext(sparkConf);
        imputation = new ImputationTransformation(FileTypes.CSV);
        imputationOfTSV = new ImputationTransformation(FileTypes.TSV);
        getLogger("org").setLevel(Level.OFF);
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

        ReplaceProcessor replaceProcessor = new ReplaceProcessor(FileTypes.CSV);
        JavaRDD<String> replacedDataset = replaceProcessor.replace(initialDataset, replacer);

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