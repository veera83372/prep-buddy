package org.apache.prepbuddy.python.connector;

import org.apache.spark.api.java.function.Function;

public class StringToBytes implements Function<String,byte[]> {
    @Override
    public byte[] call(String element) throws Exception {
        byte keyBytes[] = element.getBytes("UTF-8");
        return keyBytes;
    }
}