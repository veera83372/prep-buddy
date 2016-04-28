package org.apache.prepbuddy.transformations;

import org.apache.spark.api.java.JavaRDD;

import java.security.NoSuchAlgorithmException;

public interface DataTransformation  {
    JavaRDD apply(JavaRDD inputData) throws NoSuchAlgorithmException;
}
