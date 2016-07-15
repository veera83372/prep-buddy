package org.apache.datacommons.prepbuddy.api.java;

import org.apache.datacommons.prepbuddy.api.JavaSparkTestCase;
import org.apache.datacommons.prepbuddy.api.java.types.FileType;
import org.apache.datacommons.prepbuddy.utils.RowRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class JavaTransformableRDDTest extends JavaSparkTestCase {

    @Test
    public void shouldBeAbleToDeduplicate() {
        JavaRDD<String> numbers = javaSparkContext.parallelize(Arrays.asList(
                "One,Two,Three",
                "One,Two,Three",
                "One,Two,Three",
                "Ten,Eleven,Twelve"
        ));
        JavaTransformableRDD javaTransformableRDD = new JavaTransformableRDD(numbers, FileType.CSV);
        JavaTransformableRDD javaRdd = javaTransformableRDD.deduplicate();
        assertEquals(2, javaRdd.count());

        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("X,Y,", "X,Y,", "XX,YY,ZZ"));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataset, FileType.CSV);

        JavaTransformableRDD purged = initialRDD.removeRows(new RowPurger() {
            @Override
            public Boolean evaluate(RowRecord record) {
                return record.valueAt(1).equals("YY");
            }
        });
        assertEquals(2, purged.count());
    }
}
