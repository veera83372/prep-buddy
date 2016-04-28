package org.apache.prepbuddy.preprocessor;

import org.apache.commons.lang.StringUtils;

import java.io.Serializable;

public class RecordTrimmer implements Serializable, PreprocessTask {
    private String delimiter;

    public RecordTrimmer(String delimiter) {
        this.delimiter = delimiter;
    }

    public String apply(String record) {
        String[] splittedRecord = record.split(delimiter);
        for (int wordPosition = 0; wordPosition < splittedRecord.length; wordPosition++)
            splittedRecord[wordPosition] = splittedRecord[wordPosition].trim();

        return StringUtils.join(splittedRecord, delimiter);
    }
}
