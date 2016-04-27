package org.apache.prepbuddy.preprocessor;

import org.apache.commons.lang.StringUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

public class RecordTrimmer implements Serializable, PreprocessTask {
    private String delimiter;

    public RecordTrimmer(String delimiter) {
        this.delimiter = delimiter;
    }

    public JavaRDD<String> apply(JavaRDD<String> inputDataset) {
        Function<String, String> trimAction = new Function<String, String>() {
            @Override
            public String call(String record) throws Exception {
                String[] splittedRecord = record.split(delimiter);
                for (int wordPosition = 0; wordPosition < splittedRecord.length; wordPosition++)
                    splittedRecord[wordPosition] = splittedRecord[wordPosition].trim();

                return StringUtils.join(splittedRecord, delimiter);
            }
        };
        return inputDataset.map(trimAction);
    }
}
