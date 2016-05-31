package org.apache.prepbuddy.utils;

public interface TransformationFunction<Input, Output> {

    Output transform(Input input);

    Output defaultValue();
}
