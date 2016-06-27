package org.apache.prepbuddy.functionaltests.specs;

import org.apache.prepbuddy.functionaltests.framework.DatasetTestResults;
import org.apache.prepbuddy.functionaltests.framework.DatasetTestSpec;
import org.apache.prepbuddy.functionaltests.framework.TestableDataset;
import org.apache.spark.api.java.JavaRDD;

public class TypeInferenceTestSpec extends DatasetTestSpec {


    public TypeInferenceTestSpec(TestableDataset dataset) {
        super(dataset);
    }

    @Override
    public DatasetTestResults executeTest(JavaRDD<String> testableRDD) {
        return null;
    }

    @Override
    public void validateTestResults() {

    }
}
