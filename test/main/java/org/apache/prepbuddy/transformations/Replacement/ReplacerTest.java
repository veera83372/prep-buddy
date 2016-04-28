package org.apache.prepbuddy.transformations.Replacement;

import org.apache.prepbuddy.preprocessor.FileTypes;
import org.apache.prepbuddy.transformations.SparkTestCase;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class ReplacerTest extends SparkTestCase {

    @Test
    public void shouldCallbackTheMissingDataHandler() {
        JavaRDD<String> initialDataset = context.parallelize(Arrays.asList("1,2,3,4"));

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

}