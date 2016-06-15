package org.apache.prepbuddy.transformers;

public interface TransformationFunction<Input, Output> {

    Output transform(Input input);

    Output defaultValue();
}
