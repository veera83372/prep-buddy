package org.apache.prepbuddy.functionaltests.specs;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.prepbuddy.functionaltests.framework.DatasetTestResults;
import org.apache.prepbuddy.functionaltests.framework.DatasetTestSpec;
import org.apache.prepbuddy.functionaltests.framework.TestReport;
import org.apache.prepbuddy.functionaltests.framework.TestableDataset;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.util.ArrayList;
import java.util.List;

public class FunctionalTestRunner {
    protected transient JavaSparkContext javaSparkContext;
    private transient List<DatasetTestSpec> specs = new ArrayList<>();

    public FunctionalTestRunner(String masterURL) {
        setupSparkContext(masterURL);
    }

    private void setupSparkContext(String master) {
        SparkConf sparkConf = new SparkConf().setAppName(getClass().getName()).setMaster(master);
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
        javaSparkContext = new JavaSparkContext(sparkConf);
    }

    public static void main(String[] args) {
        FunctionalTestRunner testRunner = new FunctionalTestRunner("local");
        TestableDataset dataset = new TestableDataset("data/calls.csv");
        TestableDataset personData = new TestableDataset("data/person_data/person_data.tsv");
        TypeInferenceTestSpec typeInferenceTestSpec = new TypeInferenceTestSpec(dataset);
        PercentageOfMissingDataTestSpec percentageOfMissingDataTestSpec = new PercentageOfMissingDataTestSpec(personData);

        testRunner.addSpec(typeInferenceTestSpec);
        testRunner.addSpec(percentageOfMissingDataTestSpec);

        testRunner.run();
        testRunner.printResults();
        testRunner.shutDown();
    }

    public void printResults() {
        TestReport testReport = new TestReport();
        for (DatasetTestSpec spec : specs) {
            DatasetTestResults testResults = spec.getTestResults();
            testReport.add(testResults);
        }
        testReport.show();
    }

    public void shutDown() {
        javaSparkContext.close();
    }

    public void run() {
        for (DatasetTestSpec spec : specs) {
            spec.execute(javaSparkContext);
        }
    }

    public void addSpec(DatasetTestSpec testSpec) {
        specs.add(testSpec);
    }


}
