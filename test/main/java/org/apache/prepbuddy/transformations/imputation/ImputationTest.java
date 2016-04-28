package org.apache.prepbuddy.transformations.imputation;

import org.apache.log4j.Level;
import org.apache.prepbuddy.preprocessor.FileTypes;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;

import static org.apache.log4j.Logger.getLogger;

public class ImputationTest implements Serializable{
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
        JavaRDD<String> initialDataset = ctx.parallelize(Arrays.asList(",,4,5"));
        Imputers imputers = new Imputers();
        imputers.add(0, new Imputers.HandlerFunction() {
            @Override
            public Object handleMissingField() {
                return "1234567890";
            }
        });
        imputers.add(1, new Imputers.HandlerFunction() {
            @Override
            public Object handleMissingField() {
                return "000000";
            }
        });

        JavaRDD<String> transformed = imputation.handleMissingFields(initialDataset, imputers);

        String expected = "1234567890,000000,4,5";
        String actual = transformed.first();
        Assert.assertEquals(expected, actual);
    }

    @Test(expected = SparkException.class)
    public void shouldThrowExceptionIfIndexIsInvalid() {
        JavaRDD<String> initialDataset = ctx.parallelize(Arrays.asList("1,,4,5"));
        Imputers imputers = new Imputers();
        imputers.add(6, new Imputers.HandlerFunction() {
            @Override
            public Object handleMissingField() {
                return "1";
            }
        });

        JavaRDD<String> transformed = imputation.handleMissingFields(initialDataset, imputers);
        String expected = "1,1,4,5";
        String actual = transformed.first();
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void shouldRemoveTheEntireRowWhenDataIsMissing() {
        JavaRDD<String> initialDataset = ctx.parallelize(Arrays.asList("1,,4,5","3,5,6"));
        Remover remover = new Remover();
        remover.onColumn(0);
        remover.onColumn(1);

        JavaRDD<String> transformed = imputation.removeIfNull(initialDataset, remover);

        String expected = "3,5,6";
        String actual = transformed.first();
        Assert.assertEquals(expected, actual);
    }




    @Test(expected = SparkException.class)
    public void shouldThrowExceptionWhenInvalidColumnIndexIsGivenToRemover() {
        JavaRDD<String> initialDataset = ctx.parallelize(Arrays.asList("1,,4,5","3,5,6"));
        Remover remover = new Remover();
        remover.onColumn(10);
        remover.onColumn(1);

        JavaRDD<String> transformed = imputation.removeIfNull(initialDataset, remover);
        transformed.first();
    }

    @Test(expected = SparkException.class)
    public void shouldThrowExceptionWhenLessThenZeroColumnIndexIsGivenToRemover() {
        JavaRDD<String> initialDataset = ctx.parallelize(Arrays.asList("1,,4,5","3,5,6"));
        Remover remover = new Remover();
        remover.onColumn(-1);
        remover.onColumn(1);

        imputation.removeIfNull(initialDataset, remover).first();
    }

    @Test
    public void shouldCallbackForTSVData() {
        JavaRDD<String> initialDataset = ctx.parallelize(Arrays.asList("2\t \t5"));
        Imputers imputers = new Imputers();
        imputers.add(1, new Imputers.HandlerFunction() {
            @Override
            public Object handleMissingField() {
                return "000000";
            }
        });

        JavaRDD<String> transformed = imputationOfTSV.handleMissingFields(initialDataset, imputers);

        String expected = "2\t000000\t5";
        String actual = transformed.first();
        Assert.assertEquals(expected, actual);
    }

//    @Test
//    public void replaceWith_shouldReplaceColumnValueWithTheGivenValue() {
//        JavaRDD<String> initialDataset = ctx.parallelize(Arrays.asList("1,2,4,5"));
//        Imputers imputers = new Imputers();
//        imputers.add(0, new Imputers.HandlerFunction() {
//            @Override
//            public Object handleMissingField() {
//                return "10";
//            }
//        },"1");
//        imputers.add(1, new Imputers.HandlerFunction() {
//            @Override
//            public Object handleMissingField() {
//                return "100";
//            }
//        },"5");

//        JavaRDD<String> transformed = imputation.handleMissingFields(initialDataset, imputers);
//        String expected = "10,2,4,5";
//        String actual = transformed.first();
//        Assert.assertEquals(expected, actual);
//    }

    @After
    public void tearDown() throws Exception {
        ctx.stop();
    }
}
