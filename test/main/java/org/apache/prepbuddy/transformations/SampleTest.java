package main.java.org.apache.prepbuddy.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

public class SampleTest {

    public void shouldRunTheProgram() {
        SparkConf sparkConf = new SparkConf().setAppName("Sample");
        JavaSparkContext ctx = new JavaSparkContext(sparkConf);

    }

}
