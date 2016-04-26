package org.apache.prepbuddy.preprocessor;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.catalyst.expressions.Attribute;

import java.util.ArrayList;
import java.util.List;

public class PreprocessesConfig{

    private FileTypes fileType;
    private final ArrayList<PreprocessTask> preprocessTasks;

    public PreprocessesConfig(FileTypes fileType) {
        this.fileType = fileType;
        preprocessTasks = new ArrayList<PreprocessTask>();
    }


    public PreprocessesConfig trimEachColumn() {
        preprocessTasks.add(new Trimmer(fileType.getDelimiter()));
        return this;
    }

    public JavaRDD<String> performTask(JavaRDD<String> givenRDD) {
        JavaRDD<String> resultRDD = givenRDD;
        for (PreprocessTask preprocessTask : preprocessTasks) {
             resultRDD = preprocessTask.apply(resultRDD);
        };
        return resultRDD;
    }
}
