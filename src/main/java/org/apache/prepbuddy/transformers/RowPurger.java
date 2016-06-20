package org.apache.prepbuddy.transformers;

import org.apache.prepbuddy.analyzers.FileType;
import org.apache.prepbuddy.utils.RowRecord;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

public class RowPurger implements Serializable {

    private final Predicate condition;

    public RowPurger(Predicate condition) {
        this.condition = condition;
    }

    public JavaRDD<String> apply(JavaRDD<String> dataset, final FileType type) {
        return dataset.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String record) throws Exception {
                return !condition.evaluate(new RowRecord(type.parseRecord(record)));
            }
        });
    }


    public interface Predicate extends Serializable {
        Boolean evaluate(RowRecord record);
    }
}
