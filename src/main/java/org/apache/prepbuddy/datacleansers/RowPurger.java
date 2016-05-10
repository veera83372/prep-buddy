package org.apache.prepbuddy.datacleansers;

import org.apache.prepbuddy.coreops.RowTransformation;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

public class RowPurger implements RowTransformation, Serializable{

    private final Predicate condition;

    public RowPurger(Predicate condition) {
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


    public interface Predicate extends Serializable {
        Boolean evaluate(String record);
    }
}
