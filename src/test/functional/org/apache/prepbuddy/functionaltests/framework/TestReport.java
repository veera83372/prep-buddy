package org.apache.prepbuddy.functionaltests.framework;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

public class TestReport implements Serializable {

    private List<DatasetTestResults> results = new ArrayList<>();

    public void add(DatasetTestResults testResults) {
        results.add(testResults);
    }

    public void show() {
        for (DatasetTestResults result : results) {
            System.out.println("result = " + result.getOutcome());
        }
    }
}
