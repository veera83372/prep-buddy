package org.apache.prepbuddy.functionaltests.framework;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public abstract class DatasetTestSpec {
    protected TestableDataset dataset;
    protected DatasetTestResults testResults;

    public DatasetTestSpec(TestableDataset dataset) {
        this.dataset = dataset;
    }

    public void execute(JavaSparkContext javaSparkContext) {
        JavaRDD<String> testableRDD = javaSparkContext.textFile(dataset.fileName());
        testResults = new DatasetTestResults(getClass().getCanonicalName());
        try {
            executeTest(testableRDD);
            testResults.markAsSuccess();
        } catch (AssertionError error) {
            testResults.markAsFailure(error);
        }
    }

    public DatasetTestResults getTestResults() {
        return testResults;
    }

    public abstract void executeTest(JavaRDD<String> testableRDD);


}
