package org.apache.prepbuddy.functionaltests.specs;

import org.apache.prepbuddy.functionaltests.framework.DatasetAssertions;
import org.apache.prepbuddy.functionaltests.framework.DatasetTestSpec;
import org.apache.prepbuddy.functionaltests.framework.TestableDataset;
import org.apache.prepbuddy.qualityanalyzers.DataType;
import org.apache.prepbuddy.rdds.AnalyzableRDD;
import org.apache.spark.api.java.JavaRDD;

public class TypeInferenceTestSpec extends DatasetTestSpec {

    public TypeInferenceTestSpec(TestableDataset dataset) {
        super(dataset);
    }

    @Override
    public void executeTest(JavaRDD<String> testableRDD) {
        AnalyzableRDD analyzableRDD = new AnalyzableRDD(testableRDD);
        analyzableRDD.cache();
        DataType actualDataType = analyzableRDD.inferType(0);
        DatasetAssertions.assertType(DataType.MOBILE_NUMBER, actualDataType);
    }
}
