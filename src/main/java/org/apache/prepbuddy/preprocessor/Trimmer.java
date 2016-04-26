package org.apache.prepbuddy.preprocessor;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

public class Trimmer implements Serializable, PreprocessTask {
    private String delimiter;

    public Trimmer(String delimiter) {
        this.delimiter = delimiter;
    }

    public JavaRDD<String> apply(JavaRDD<String> rdd) {
        Function<String, String> trimAction = new Function<String, String>() {
            @Override
            public String call(String record) throws Exception {
                String[] splittedRecord = record.split(delimiter);
                for (int wordPosition = 0; wordPosition < splittedRecord.length; wordPosition++) {
                    splittedRecord[wordPosition] = splittedRecord[wordPosition].trim();
                }
                return join(splittedRecord);
            }
        };
        return rdd.map(trimAction);
    }

    private String join(String[] words) {
        if(words.length == 1)
            return words[0];

        String resultString = words[0];
        for (int wordPosition = 1; wordPosition < words.length; wordPosition++){
            resultString += delimiter+words[wordPosition];
        }
        return resultString;
    }
}
