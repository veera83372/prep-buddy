package org.apache.prepbuddy.transformations.imputation;

import org.apache.prepbuddy.preprocessor.FileTypes;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

import static org.apache.commons.lang.StringUtils.join;

public class ImputationTransformation implements Serializable {

    private FileTypes fileType;

    public ImputationTransformation(FileTypes fileType) {
        this.fileType = fileType;
    }

    public JavaRDD<String> handleMissingFields(JavaRDD<String> dataset, final Imputers imputers) {
        JavaRDD<String> transformedRDD = dataset.map(new Function<String, String>() {
            @Override
            public String call(String row) throws Exception {
                String delimiter = fileType.getDelimiter();
                String[] columns = row.split(delimiter);
                String[] transformedColumns = imputers.handle(columns);
                return join(transformedColumns, delimiter);
            }
        });
        return transformedRDD;
    }


    public JavaRDD<String> removeIfNull(JavaRDD<String> initialDataset, final Remover remover) {
        return initialDataset.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String row) throws Exception {
                String[] column = row.split(fileType.getDelimiter());
                return remover.hasFieldsValue(column);
            }
        });
    }
}
