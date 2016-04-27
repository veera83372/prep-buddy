package org.apache.prepbuddy.preprocessor;

import org.apache.spark.api.java.JavaRDD;

import java.util.ArrayList;

public class PreprocessConfig {

    private FileTypes fileType;
    private final ArrayList<PreprocessTask> preprocessTasks;

    public PreprocessConfig(FileTypes fileType) {
        this.fileType = fileType;
        preprocessTasks = new ArrayList<PreprocessTask>();
    }


    public PreprocessConfig trimEachColumn() {
        RecordTrimmer trimmingTask = new RecordTrimmer(fileType.getDelimiter());
        preprocessTasks.add(trimmingTask);
        return this;
    }

    public JavaRDD<String> performTask(JavaRDD<String> givenRDD) {
        JavaRDD<String> resultRDD = givenRDD;
        for (PreprocessTask preprocessTask : preprocessTasks)
             resultRDD = preprocessTask.apply(resultRDD);

        return resultRDD;
    }
}
