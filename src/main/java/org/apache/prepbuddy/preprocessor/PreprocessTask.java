package org.apache.prepbuddy.preprocessor;

import org.apache.spark.api.java.JavaRDD;

public interface PreprocessTask {
    JavaRDD<String> apply(JavaRDD<String> inputDataset);
}
