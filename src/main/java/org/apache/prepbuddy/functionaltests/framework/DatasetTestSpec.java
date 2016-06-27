package org.apache.prepbuddy.functionaltests.framework;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public abstract class DatasetTestSpec {
    protected TestableDataset dataset;
    private DatasetTestResults testResults;

    public DatasetTestSpec(TestableDataset dataset) {
        this.dataset = dataset;
    }

    public void execute(JavaSparkContext javaSparkContext) {
        JavaRDD<String> testableRDD = javaSparkContext.textFile(dataset.fileName());
        testResults = executeTest(testableRDD);
    }

    public abstract DatasetTestResults executeTest(JavaRDD<String> testableRDD);


    public abstract void validateTestResults();
}
