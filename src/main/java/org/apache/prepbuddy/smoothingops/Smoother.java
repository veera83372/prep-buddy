package org.apache.prepbuddy.smoothingops;

import org.apache.spark.api.java.JavaRDD;

import java.io.Serializable;

public interface Smoother extends Serializable {
    JavaRDD<Double> smooth(JavaRDD<String> singleColumnRdd);
}
