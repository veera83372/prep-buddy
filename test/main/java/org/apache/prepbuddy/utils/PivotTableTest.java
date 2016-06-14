package org.apache.prepbuddy.utils;

import org.apache.prepbuddy.SparkTestCase;
import org.apache.prepbuddy.datacleansers.imputation.Probability;
import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.transformers.TransformationFunction;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class PivotTableTest extends SparkTestCase {
    @Test
    public void shouldGiveDefaultValueWhenColumnKeyIsNotPresent() {
        PivotTable<Integer> pivotTable = new PivotTable<>(0);
        pivotTable.addEntry("firstRow", "firstColumn", 5);

        int value = pivotTable.valueAt("firstRow", "secondColumn");
        assertEquals(0, value);
    }

    @Test
    public void shouldTransformThePivotTable() {
        JavaRDD<String> initialDataSet = javaSparkContext.parallelize(Arrays.asList(
                "known,new,long,home,skips",
                "unknown,new,short,work,reads",
                "unknown,follow Up,long,work,skips",
                "known,follow Up,long,home,skips",
                "known,new,short,home,reads",
                "known,follow Up,long,work,skips",
                "unknown,follow Up,short,work,skips",
                "unknown,new,short,work,reads",
                "known,follow Up,long,home,skips",
                "known,new,long,work,skips",
                "unknown,follow Up,short,home,skips",
                "known,new,long,work,skips",
                "known,follow Up,short,home,reads",
                "known,new,short,work,reads",
                "known,new,short,home,reads",
                "known,follow Up,short,work,reads",
                "known,new,short,home,reads",
                "unknown,new,short,work,reads"
        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataSet);
        final long count = initialRDD.count();
        PivotTable<Integer> pivotTable = initialRDD.pivotByCount(4, new int[]{0, 1, 2, 3});
        PivotTable<Probability> transform = (PivotTable<Probability>) pivotTable.transform(new TransformationFunction<Integer, Probability>() {
            @Override
            public Probability transform(Integer integer) {
                return new Probability((double) integer.intValue() / count);
            }

            @Override
            public Probability defaultValue() {
                return new Probability(0);
            }
        });
        Probability expected = new Probability((double) 7 / 18);
        Probability actual = transform.valueAt("skips", "long");

        assertEquals(expected, actual);

        Probability expectedZero = new Probability(0);
        assertEquals(expectedZero, transform.valueAt("reads", "long"));
    }
}