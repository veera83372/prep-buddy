package org.apache.prepbuddy.pythonConnector;

import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.mllib.linalg.DenseVector;
import scala.Tuple2;

import java.nio.ByteBuffer;

public class BytesToKeyAndSeries implements PairFunction {

    @Override
    public Tuple2<String, DenseVector> call(Object o) throws Exception {
        byte[] arr = (byte[]) o;
        ByteBuffer buf = ByteBuffer.wrap(arr);
        int keySize = buf.getInt();
        byte[] keyBytes = new byte[keySize];
        buf.get(keyBytes);
        int seriesSize = buf.getInt();
        double[] series = new double[seriesSize];
        for (int i = 0; i < seriesSize; i++) {
            series[i] = buf.getDouble();
        }
        return new Tuple2<>(new String(keyBytes, "UTF8"), new DenseVector(series));
    }
}