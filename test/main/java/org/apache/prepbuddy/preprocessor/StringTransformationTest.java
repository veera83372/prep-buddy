package org.apache.prepbuddy.preprocessor;

import org.apache.prepbuddy.preprocessor.Replacement.ReplaceHandler;
import org.apache.prepbuddy.preprocessor.Replacement.Replacer;
import org.apache.prepbuddy.transformations.SparkTestCase;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class StringTransformationTest extends SparkTestCase {

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

        StringTransformation preprocessesConfig = new StringTransformation(FileType.CSV);
        List<String> result = preprocessesConfig
                                    .trimEachColumn()
                                    .apply(csvInput)
                                    .collect();

        assertStringList(expected,result);
    }

    @Test
    public void performsTrimmingAndReplacementTaskForTheGivenTsvTypeOfData() {
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
                "10\t123\tOutgoing\t0\tThu Sep 09 18:16:47 +0100 2010",
                "10\t123\tOutgoing\t0\tFri Sep 10 06:04:43 +0100 2010",
                "10\t123\tIncoming\t474\tThu Sep 09 18:44:34 +0100 2010",
                "10\t123\tOutgoing\t0\tMon Sep 13 13:54:40 +0100 2010",
                "10\t123\tOutgoing\t2\tTue Sep 14 14:48:37 +0100 2010"
        );

        StringTransformation preprocessesConfig = new StringTransformation(FileType.TSV);
        List<String> result = preprocessesConfig
                .trimEachColumn()
                .addReplaceHandlers(
                        new ReplaceHandler(0, (Replacer.ReplaceFunction<String, String>) value -> "10"),
                        new ReplaceHandler(1, (Replacer.ReplaceFunction<String, String>) value -> "123")
                )
                .apply(tsvInput)
                .collect();

        assertStringList(expected,result);
    }
}