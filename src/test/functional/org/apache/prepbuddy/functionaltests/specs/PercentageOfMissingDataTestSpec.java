package org.apache.prepbuddy.functionaltests.specs;

import org.apache.prepbuddy.functionaltests.framework.DatasetAssertions;
import org.apache.prepbuddy.functionaltests.framework.DatasetTestSpec;
import org.apache.prepbuddy.functionaltests.framework.TestableDataset;
import org.apache.prepbuddy.qualityanalyzers.AnalysisPlan;
import org.apache.prepbuddy.qualityanalyzers.DatasetInsights;
import org.apache.prepbuddy.qualityanalyzers.FileType;
import org.apache.prepbuddy.rdds.AnalyzableRDD;
import org.apache.prepbuddy.utils.Range;
import org.apache.spark.api.java.JavaRDD;

import java.util.Arrays;

public class PercentageOfMissingDataTestSpec extends DatasetTestSpec {
    public PercentageOfMissingDataTestSpec(TestableDataset dataset) {
        super(dataset);
    }

    @Override
    public void executeTest(JavaRDD<String> testableRDD) {
        AnalyzableRDD analyzableRDD = new AnalyzableRDD(testableRDD, FileType.TSV);
        Range range = new Range(0, analyzableRDD.getNumberOfColumns() - 1);
        AnalysisPlan analysisPlan = new AnalysisPlan(range, Arrays.asList("\\N", "N/A"));
        DatasetInsights datasetInsights = analyzableRDD.analyzeColumns(analysisPlan);

        DatasetAssertions.assertEquals(new Double(0), datasetInsights.percentageOfMissingValue(0));
    }
}
