package org.apache.prepbuddy.reformatters;

import org.apache.spark.api.java.JavaRDD;

public interface RowTransformation {
    JavaRDD<String> apply(JavaRDD<String> dataset);
}
