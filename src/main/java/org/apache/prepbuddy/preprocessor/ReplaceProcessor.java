package org.apache.prepbuddy.preprocessor;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

import static org.apache.commons.lang.StringUtils.join;

public class ReplaceProcessor implements Serializable {
    private FileTypes fileType;

    public ReplaceProcessor(FileTypes fileType) {
        this.fileType = fileType;
    }

    public JavaRDD<String> replace(JavaRDD<String> initialDataset, final Replacer replacer) {
        return initialDataset.map(new Function<String, String>() {
            @Override
            public String call(String row) throws Exception {
                String[] replaced = replacer.replaceValue(row.split(fileType.getDelimiter()));
                return join(replaced, fileType.getDelimiter());
            }
        });
    }
}
