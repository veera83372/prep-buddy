package org.apache.prepbuddy.functionaltests.specs;


import org.apache.prepbuddy.functionaltests.framework.DatasetTestResults;
import org.apache.prepbuddy.functionaltests.framework.FunctionalTest;
import org.apache.prepbuddy.functionaltests.framework.TestableDataset;

public class FunctionalTestRunner {

    public static void main(String[] args) {
        FunctionalTest typeAnalysisTest = new FunctionalTest();
        DatasetTestResults actual = typeAnalysisTest
                .withSpec(new TypeInferenceTestSpec())
                .on(new TestableDataset("filename"))
                .run();

    }


}
