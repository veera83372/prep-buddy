package org.apache.prepbuddy.datacleansers.imputation;

import org.apache.prepbuddy.SparkTestCase;
import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.utils.RowRecord;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

import static junit.framework.Assert.assertEquals;

public class NaiveBayesClassifierTest extends SparkTestCase {
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
        NaiveBayesClassifier naiveBayesClassifier = new NaiveBayesClassifier(0, 1, 2, 3);

        naiveBayesClassifier.train(initialRDD, 4);
        String[] rowRecord = ("sunny,cool,high,false").split(",");
        String mostProbable = naiveBayesClassifier.makeDecision(new RowRecord(rowRecord));

        assertEquals("N", mostProbable);
        rowRecord = ("rain,hot,high,false,N").split(",");
        assertEquals("N", naiveBayesClassifier.makeDecision(new RowRecord(rowRecord)));

    }
}