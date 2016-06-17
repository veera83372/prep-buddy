package org.apache.prepbuddy.pythonConnector;

import org.apache.spark.api.java.function.Function;

public class BytesToString implements Function<byte[],String> {
    @Override
    public String call(byte[] bytes) throws Exception {
        return new String(bytes,"UTF-8");
    }
}