package org.apache.prepbuddy.transformations.imputation;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

import static org.apache.commons.lang.StringUtils.join;

public class ImputationTransformation implements Serializable {

    public JavaRDD<String> handleMissingFields(JavaRDD<String> dataset, final Imputers imputers) {
        JavaRDD<String> transformedRDD = dataset.map(new Function<String, String>() {
            @Override
            public String call(String row) throws Exception {
                String[] columns = row.split(",");
                String[] transformedColumns = imputers.handle(columns);
                return join(transformedColumns, ",");
            }
        });
        return transformedRDD;
    }

}
