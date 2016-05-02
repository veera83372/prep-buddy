package org.apache.prepbuddy.preprocessor.replacement;

import java.io.Serializable;

public class ReplaceHandler implements Serializable {
    final private Integer index;
    final private Replacer.ReplaceFunction<String, String> replaceFunction;

    public ReplaceHandler(Integer index, Replacer.ReplaceFunction<String, String> replaceFunction) {
        this.index = index;
        this.replaceFunction = replaceFunction;
    }

    public String[] replace(String[] record){
        String recordToReplace = record[index];
        String replaceValue = replaceFunction.replace(recordToReplace);
        record[index] = replaceValue;
        return record;
    }

}
