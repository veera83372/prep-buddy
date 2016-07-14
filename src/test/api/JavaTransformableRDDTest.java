import org.apache.datacommons.prepbuddy.api.java.JavaTransformableRDD;
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

import static junit.framework.Assert.assertEquals;

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


}
