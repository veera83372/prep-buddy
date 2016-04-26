package org.apache.prepbuddy.transformations.preprocessor;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.assertEquals;

public class PreprocessesConfigTest {
    private JavaSparkContext context;

    @Before
    public void setUp() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName("Preprocessing").setMaster("local");
        context = new JavaSparkContext(sparkConf);
    }

    @After
    public void tearDown() throws Exception {
        context.close();
    }

    private void assertStringList(List<String> expected, List<String> resultFound){
        int counter = 0;
        for (String record : resultFound) {
            assertEquals(expected.get(counter++), record);
        }
    }

    @Test
    public void performsAllTheGivenPreprocessForTheGivenCsvTypeOfData() {
        JavaRDD<String> csvInput = context.parallelize(
                Arrays.asList(
                        "  07110730864,07209670163  ,Outgoing,0,Thu Sep 09 18:16:47 +0100 2010 ",
                        "    07110730864,07209670163,Outgoing   ,0,Fri Sep 10 06:04:43 +0100 2010",
                        "07784425582,07981267897 ,Incoming,474,  Thu Sep 09 18:44:34 +0100 2010",
                        "07607124303,2327,Outgoing,0, Mon Sep 13 13:54:40 +0100 2010   ",
                        "   07607124303, 07167454533,   Outgoing ,2,Tue Sep 14 14:48:37 +0100 2010   "
                )
        );

        List<String> expected = Arrays.asList(
                "07110730864,07209670163,Outgoing,0,Thu Sep 09 18:16:47 +0100 2010",
                "07110730864,07209670163,Outgoing,0,Fri Sep 10 06:04:43 +0100 2010",
                "07784425582,07981267897,Incoming,474,Thu Sep 09 18:44:34 +0100 2010",
                "07607124303,2327,Outgoing,0,Mon Sep 13 13:54:40 +0100 2010",
                "07607124303,07167454533,Outgoing,2,Tue Sep 14 14:48:37 +0100 2010"
        );

        PreprocessesConfig preprocessesConfig = new PreprocessesConfig(FileTypes.CSV);
        List<String> result = preprocessesConfig
                                    .trimEachColumn()
                                    .performTask(csvInput)
                                    .collect();

        assertStringList(expected,result);
    }

    @Test
    public void performsAllTheGivenPreprocessForTheGivenTsvTypeOfData() {
        JavaRDD<String> tsvInput = context.parallelize(
                Arrays.asList(
                        "  07110730864\t07209670163 \tOutgoing  \t0\t   Thu Sep 09 18:16:47 +0100 2010 ",
                        "    07110730864\t07209670163\tOutgoing \t0\tFri Sep 10 06:04:43 +0100 2010",
                        "07784425582\t07981267897\t   Incoming\t474\tThu Sep 09 18:44:34 +0100 2010",
                        "  07607124303  \t2327  \tOutgoing\t0\tMon Sep 13 13:54:40 +0100 2010   ",
                        "   07607124303\t07167454533  \t  Outgoing  \t2\tTue Sep 14 14:48:37 +0100 2010   "
                )
        );
        List<String> expected = Arrays.asList(
                "07110730864\t07209670163\tOutgoing\t0\tThu Sep 09 18:16:47 +0100 2010",
                "07110730864\t07209670163\tOutgoing\t0\tFri Sep 10 06:04:43 +0100 2010",
                "07784425582\t07981267897\tIncoming\t474\tThu Sep 09 18:44:34 +0100 2010",
                "07607124303\t2327\tOutgoing\t0\tMon Sep 13 13:54:40 +0100 2010",
                "07607124303\t07167454533\tOutgoing\t2\tTue Sep 14 14:48:37 +0100 2010"
        );

        PreprocessesConfig preprocessesConfig = new PreprocessesConfig(FileTypes.TSV);
        List<String> result = preprocessesConfig
                .trimEachColumn()
                .performTask(tsvInput)
                .collect();

        assertStringList(expected,result);
    }
}