package org.apache.prepbuddy.transformation;

import org.apache.prepbuddy.filetypes.FileType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

public class MarkingTransformation implements Serializable {
    private final MarkerPredicate condition;
    private final String symbol;

    public MarkingTransformation(MarkerPredicate condition, String symbol) {
        this.condition = condition;
        this.symbol = symbol;
    }

    public JavaRDD<String> apply(JavaRDD<String> dataset, FileType type) {
        return dataset.map(new Function<String, String>() {
            @Override
            public String call(String row) throws Exception {
                String newRow = type.appendDelimeter(row);
                if (condition.evaluate(newRow))
                    return newRow + symbol;
                return newRow;
            }
        });
    }
    public interface MarkerPredicate extends Serializable {
        boolean evaluate(String row);
    }
}
