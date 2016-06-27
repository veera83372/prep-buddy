package org.apache.prepbuddy.functionaltests.framework;

public class TestableDataset {
    private String fileName;

    public TestableDataset(String fileName) {

        this.fileName = fileName;
    }

    public String fileName() {
        return fileName;
    }
}
