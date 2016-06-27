package org.apache.prepbuddy.functionaltests.framework;

public class DatasetTestResults {
    private String result;
    private String testSpecName;
    private AssertionError cause;

    public DatasetTestResults(String testSpecName) {
        this.testSpecName = testSpecName;
    }

    public void markAsSuccess() {
        result = "SUCCESSFUL";
    }

    public void markAsFailure(AssertionError error) {
        result = "FAILURE";
        cause = error;
    }

    public String getOutcome() {
        return result;
    }

    public String getCause() {
        return cause.getMessage();
    }
}
