import org.apache.datacommons.prepbuddy.api.java.JavaTransformableRDD;
import org.apache.datacommons.prepbuddy.imputations.MeanSubstitution;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.Serializable;
import java.util.Arrays;
import java.util.List;

import static junit.framework.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class JavaTransformableRDDTest implements Serializable {
    protected transient JavaSparkContext javaSparkContext;

    @Before
    public void setUp() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName(getClass().getName()).setMaster("local");
        javaSparkContext = new JavaSparkContext(sparkConf);
        Logger.getLogger("org").setLevel(Level.OFF);
        Logger.getLogger("akka").setLevel(Level.OFF);
    }

    @After
    public void tearDown() throws Exception {
        javaSparkContext.close();
    }

    @Test
    public void shouldBeAbleToDeduplicate() {
        JavaRDD<String> numbers = javaSparkContext.parallelize(Arrays.asList(
                "One,Two,Three",
                "One,Two,Three",
                "One,Two,Three",
                "Ten,Eleven,Twelve"
        ));
        JavaTransformableRDD javaTransformableRDD = new JavaTransformableRDD(numbers);
        JavaTransformableRDD javaRdd = javaTransformableRDD.deduplicate();
        assertEquals(2, javaRdd.count());
    }

    @Test
    public void shouldImputeTheValueWithTheMean() {
        JavaRDD<String> initialDataSet = javaSparkContext.parallelize(Arrays.asList(
                "07434677419,07371326239,Incoming,31,Wed Sep 15 19:17:44 +0100 2010",
                "07641036117,01666472054,Outgoing,20,Mon Feb 11 07:18:23 +0000 1980",
                "07641036117,07371326239,Incoming, ,Mon Feb 11 07:45:42 +0000 1980",
                "07641036117,07681546436,Missed,12,Mon Feb 11 08:04:42 +0000 1980"

        ));
        JavaTransformableRDD initialRDD = new JavaTransformableRDD(initialDataSet);
        JavaTransformableRDD imputed = initialRDD.impute(3, new MeanSubstitution());
        List<String> listOfRecord = imputed.collect();
        assertEquals(4, listOfRecord.size());
        String expected = "07641036117,07371326239,Incoming,21.0,Mon Feb 11 07:45:42 +0000 1980";
        assertTrue(listOfRecord.contains(expected));
    }
}
