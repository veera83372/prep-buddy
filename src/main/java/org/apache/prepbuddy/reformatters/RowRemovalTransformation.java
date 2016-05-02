package org.apache.prepbuddy.reformatters;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

public class RowRemovalTransformation implements RowTransformation, Serializable {

    private final RowPredicate condition;

    public RowRemovalTransformation(RowPredicate condition) {
        this.condition = condition;
    }

    @Override
    public JavaRDD<String> apply(JavaRDD<String> dataset) {
        return dataset.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String record) throws Exception {
                return !condition.evaluate(record);
            }
        });

    }
}
