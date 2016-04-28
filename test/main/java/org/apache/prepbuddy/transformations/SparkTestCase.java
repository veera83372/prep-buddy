package org.apache.prepbuddy.transformations;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Before;

import java.io.Serializable;

public class SparkTestCase implements Serializable {
    protected JavaSparkContext context;


    @Before
    public void setUp() throws Exception {
        SparkConf sparkConf = new SparkConf().setAppName(getClass().getName()).setMaster("local");
        context = new JavaSparkContext(sparkConf);
    }

    @After
    public void tearDown() throws Exception {
        context.close();
    }
}
