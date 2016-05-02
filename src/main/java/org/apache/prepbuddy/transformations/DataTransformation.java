package org.apache.prepbuddy.transformations;

import org.apache.prepbuddy.DatasetTransformations;
import org.apache.prepbuddy.preprocessor.FileType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import scala.Serializable;

public class DataTransformation implements Serializable{

    public JavaRDD<String> apply(JavaRDD<String> initialDataset,
                                 final DatasetTransformations datasetTransformations, final FileType type) {

        JavaRDD<String> deduplicateDataset = datasetTransformations.applyRowTransforms(initialDataset);

        JavaRDD<String> finalDataset = deduplicateDataset.map(new Function<String, String>() {
            @Override
            public String call(String record) throws Exception {
                String[] untransformedColumns = type.parseRecord(record);
                String[] transformedColumns = datasetTransformations.applyColumnTransforms(untransformedColumns);
                return type.join(transformedColumns);
            }
        });
        return finalDataset;
    }
}
