package org.apache.prepbuddy.preprocessor.replacement;

import org.apache.commons.lang.StringUtils;
import org.apache.prepbuddy.preprocessor.PreprocessTask;

import java.io.Serializable;
import java.util.ArrayList;

public class Replacer implements Serializable, PreprocessTask {
    private String delimiter;
    private final ArrayList<ReplaceHandler> replaceHandlers;

    public Replacer(String delimiter) {
        this.delimiter = delimiter;
        replaceHandlers = new ArrayList<ReplaceHandler>();
    }

    @Override
    public String apply(String record) {
        String[] splittedColumns = record.split(delimiter);
        for (ReplaceHandler replaceHandler : replaceHandlers) {
            splittedColumns = replaceHandler.replace(splittedColumns);
        }
        return StringUtils.join(splittedColumns, delimiter);
    }

    public void add(ReplaceHandler handler) {
        replaceHandlers.add(handler);
    }

    public interface ReplaceFunction<T, T1> extends Serializable {
        String replace(String value);
    }
}
