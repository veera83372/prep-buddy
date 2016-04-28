package org.apache.prepbuddy.preprocessor;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;
import java.util.ArrayList;

public class StringTransformation implements Serializable{

    private FileTypes fileType;
    private final ArrayList<PreprocessTask> preprocessTasks;

    public StringTransformation(FileTypes fileType) {
        this.fileType = fileType;
        preprocessTasks = new ArrayList<PreprocessTask>();
    }


    public StringTransformation trimEachColumn() {
        RecordTrimmer trimmingTask = new RecordTrimmer(fileType.getDelimiter());
        preprocessTasks.add(trimmingTask);
        return this;
    }

    public JavaRDD<String> apply(JavaRDD<String> dataset) {
        return dataset.map(new Function<String, String>() {
            @Override
            public String call(String record) throws Exception {
                return applyAllPreprocessTasks(record);
            }
        });
    }

    private String applyAllPreprocessTasks(String record) {
        String transformedRecord = record;
        for (PreprocessTask preprocessTask : preprocessTasks) {
            transformedRecord = preprocessTask.apply(transformedRecord);
        }
        return transformedRecord;
    }
}
