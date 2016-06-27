package org.apache.prepbuddy.functionaltests.specs;


import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.prepbuddy.functionaltests.framework.DatasetTestSpec;
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
        javaSparkContext = new JavaSparkContext(sparkConf);
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
    }

    public static void main(String[] args) {
        FunctionalTestRunner testRunner = new FunctionalTestRunner(args[0]);
        TypeInferenceTestSpec typeInferenceTestSpec = new TypeInferenceTestSpec(new TestableDataset(""));
        testRunner.addSpec(typeInferenceTestSpec);
        testRunner.run();
        typeInferenceTestSpec.validateTestResults();
    }

    private void run() {
        for (DatasetTestSpec spec : specs) {
            spec.execute(javaSparkContext);
        }
    }

    public void addSpec(DatasetTestSpec testSpec) {
        specs.add(testSpec);
    }


}
