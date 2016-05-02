package org.apache.prepbuddy.preprocessor;

import org.apache.prepbuddy.preprocessor.replacement.ReplaceHandler;
import org.apache.prepbuddy.preprocessor.replacement.Replacer;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.util.ArrayList;

public class StringTransformation implements Serializable{

    private FileType fileType;
    private final ArrayList<PreprocessTask> preprocessTasks;

    public StringTransformation(FileType fileType) {
        this.fileType = fileType;
        preprocessTasks = new ArrayList<PreprocessTask>();
    }


    public StringTransformation trimEachColumn() {
        RecordTrimmer trimmingTask = new RecordTrimmer(fileType.getDelimiter());
        preprocessTasks.add(trimmingTask);
        return this;
    }

    public StringTransformation addReplaceHandlers(ReplaceHandler... replaceHandler){
        Replacer replacer = new Replacer(fileType.getDelimiter());
        for (ReplaceHandler handler : replaceHandler) {
            replacer.add(handler);
        }
        preprocessTasks.add(replacer);
        return this;
    }
    public JavaRDD<String> apply(JavaRDD<String> dataset) {
        return dataset.map((Function<String, String>) record -> applyAllPreprocessTasks(record));
    }

    private String applyAllPreprocessTasks(String record) {
        String transformedRecord = record;
        for (PreprocessTask preprocessTask : preprocessTasks) {
            transformedRecord = preprocessTask.apply(transformedRecord);
        }
        return transformedRecord;
    }
}
