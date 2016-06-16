package org.apache.prepbuddy.pythonConnector;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import scala.Tuple2;

import java.nio.ByteBuffer;

public class KeyAndSeriesToBytes implements Function {
    public void putVector(ByteBuffer buf, Vector vec) {
        buf.putInt(vec.size());
        int i = 0;
        while (i < vec.size()) {
            buf.putDouble(vec.apply(i));
            i++;
        }
    }

    @Override
    public Object call(Object v1) throws Exception {
        Tuple2<String, Vector> keyVec = (Tuple2<String, Vector>) v1;
        byte keyBytes[] = keyVec._1.getBytes("UTF-8");
        Vector vec = keyVec._2;
        int INT_SIZE = 4;
        int DOUBLE_SIZE = 8;
        byte[] arr = new byte[2 * INT_SIZE + keyBytes.length + DOUBLE_SIZE * vec.size()];
        ByteBuffer buf = ByteBuffer.wrap(arr);
        buf.putInt(keyBytes.length);
        buf.put(keyBytes);
        putVector(buf, vec);
        return arr;
    }

}