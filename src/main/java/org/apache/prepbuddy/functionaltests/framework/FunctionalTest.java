package org.apache.prepbuddy.functionaltests.framework;

public class FunctionalTest {

    private DatasetTestSpec datasetTestSpec;
    private TestableDataset testableDataset;

    public FunctionalTest withSpec(DatasetTestSpec datasetTestSpec) {
        this.datasetTestSpec = datasetTestSpec;
        return this;
    }

    public FunctionalTest on(TestableDataset filename) {
        this.testableDataset = filename;
        return this;
    }

    public DatasetTestResults run() {
        datasetTestSpec.execute(testableDataset);
        return new DatasetTestResults();
    }
}
