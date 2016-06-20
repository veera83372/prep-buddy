package org.apache.prepbuddy.cleansers.imputation;

import org.apache.prepbuddy.SparkTestCase;
import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.utils.RowRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class NaiveBayesSubstitutionTest extends SparkTestCase {
    @Test
    public void shouldMakeDecisionByGivenTrainingDataSet() {
        JavaRDD<String> initialDataSet = javaSparkContext.parallelize(Arrays.asList(
                "sunny,hot,high,false,N",
                "sunny,hot,high,true,N",
                "overcast,hot,high,false,P",
                "rain,mild,high,false,P",
                "rain,cool,normal,false,P",
                "rain,cool,normal,true,N",
                "overcast,cool,normal,true,P",
                "sunny,mild,high,false,N",
                "sunny,cool,normal,false,P",
                "rain,mild,normal,false,P",
                "sunny,mild,normal,true,P",
                "overcast,mild,high,true,P",
                "overcast,hot,normal,false,P",
                "rain,mild,high,true,N"
        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataSet);

        NaiveBayesSubstitution naiveBayesSubstitution = new NaiveBayesSubstitution(0, 1, 2, 3);
        naiveBayesSubstitution.prepareSubstitute(initialRDD, 4);
        String[] rowRecord = ("sunny,cool,high,false").split(",");
        String mostProbable = naiveBayesSubstitution.handleMissingData(new RowRecord(rowRecord));

        assertEquals("N", mostProbable);
        rowRecord = ("rain,hot,high,false").split(",");
        assertEquals("N", naiveBayesSubstitution.handleMissingData(new RowRecord(rowRecord)));

        String[] record = ("overcast, hot, high, true").split(",");
        assertEquals("P", naiveBayesSubstitution.handleMissingData(new RowRecord(record)));

    }

    @Test
    public void shouldPredictGender() {
        JavaRDD<String> initialDataSet = javaSparkContext.parallelize(Arrays.asList(
                "Drew,No,Blue,Short,Male",
                "Claudia,Yes,Brown,Long,Female",
                "Drew,No,Blue,Long,Female",
                "Drew,No,Blue,Long,Female",
                "Alberto,Yes,Brown,Short,Male",
                "Karin,No,Blue,Long,Female",
                "Nina,Yes,Brown,Short,Female",
                "Sergio,Yes,Blue,Long,Male"
        ));
        TransformableRDD initialRDD = new TransformableRDD(initialDataSet);

        NaiveBayesSubstitution naiveBayesSubstitution = new NaiveBayesSubstitution(0, 1, 2, 3);
        naiveBayesSubstitution.prepareSubstitute(initialRDD, 4);
        String[] rowRecord = ("Drew,Yes,Blue,Long,").split(",");
        String mostProbable = naiveBayesSubstitution.handleMissingData(new RowRecord(rowRecord));

        assertEquals("Female", mostProbable);

    }
}