package org.apache.prepbuddy.preprocessor;

import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;
import java.util.ArrayList;

public class StringTransformation implements Serializable {

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

    public JavaRDD<String> apply(JavaRDD<String> inputDataset) {
        JavaRDD<String> resultRDD = inputDataset;
        for (PreprocessTask preprocessTask : preprocessTasks)
             resultRDD = preprocessTask.apply(resultRDD);

        return resultRDD;
    }
}
