package org.apache.prepbuddy.transformations.imputation;

import org.apache.prepbuddy.preprocessor.FileType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

import static org.apache.commons.lang.StringUtils.join;

public class ImputationTransformation implements Serializable {

    public JavaRDD<String> handleMissingFields(JavaRDD<String> dataset, final Imputers imputers, final FileType fileType) {
        JavaRDD<String> transformedRDD = dataset.map(new Function<String, String>() {
            @Override
            public String call(String row) throws Exception {
                String[] columns = row.split(fileType.getDelimiter());
                String[] transformedColumns = imputers.handle(columns);
                return join(transformedColumns, fileType.getDelimiter());
            }
        });
        return transformedRDD;
    }


    public JavaRDD<String> removeIfNull(JavaRDD<String> initialDataset, final Remover remover, final FileType fileType) {
        return initialDataset.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String row) throws Exception {
                String[] column = row.split(fileType.getDelimiter());
                return remover.hasFieldsValue(column);
            }
        });
    }
}
