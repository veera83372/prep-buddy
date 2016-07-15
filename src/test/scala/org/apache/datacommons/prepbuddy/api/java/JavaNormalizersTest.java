package org.apache.datacommons.prepbuddy.api.java;

import org.apache.datacommons.prepbuddy.api.JavaSparkTestCase;
import org.apache.datacommons.prepbuddy.api.java.types.FileType;
import org.apache.datacommons.prepbuddy.normalizers.MinMaxNormalizer;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class JavaNormalizersTest extends JavaSparkTestCase {
    @Test
    public void shouldNormalizeRecordsUsingMinMaxNormalizer() throws Exception {
        JavaRDD<String> initialDataSet = javaSparkContext.parallelize(Arrays.asList(
                "07434677419,07371326239,Incoming,211,Wed Sep 15 19:17:44 +0100 2010",
                "07641036117,01666472054,Outgoing,0,Mon Feb 11 07:18:23 +0000 1980",
                "07641036117,07371326239,Incoming,45,Mon Feb 11 07:45:42 +0000 1980",
                "07641036117,07371326239,Incoming,45,Mon Feb 11 07:45:42 +0000 1980",
                "07641036117,07681546436,Missed,12,Mon Feb 11 08:04:42 +0000 1980"

        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataSet, FileType.CSV);
        JavaTransformableRDD finalRDD = initialRDD.normalize(3, new MinMaxNormalizer(0, 1));
        List<String> normalizedDurations = finalRDD.select(3).collect();

        List<String> expected = Arrays.asList("1.0", "0.0", "0.2132701421800948", "0.2132701421800948", "0.05687203791469194");
        assertEquals(expected, normalizedDurations);
    }
}
