package org.apache.prepbuddy.encryptors;

import com.n1analytics.paillier.EncryptedNumber;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;

public class HomomorphicEncryptedRDD<T extends EncryptedNumber> implements Serializable {

    protected HomomorphicEncryptedRDD(JavaRDD<T> rdd) {
        this.rdd = rdd;
    }

    private JavaRDD<T> rdd;
    public T reduce(Function2<T,T,T> function2) {
        return rdd.reduce(function2);
    }
}
