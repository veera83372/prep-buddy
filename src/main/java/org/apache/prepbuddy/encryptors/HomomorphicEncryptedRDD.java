package org.apache.prepbuddy.encryptors;

import com.n1analytics.paillier.EncryptedNumber;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;

import java.io.Serializable;

public class HomomorphicEncryptedRDD<T extends EncryptedNumber> implements Serializable {

    private JavaRDD<T> rdd;

    protected HomomorphicEncryptedRDD(JavaRDD<T> rdd) {
        this.rdd = rdd;
    }
    private T reduce(Function2<T,T,T> function2) {
        return rdd.reduce(function2);
    }
    public double sum(Encryptor encryptor){
        T reduce = reduce(new Function2<T, T, T>() {
            @Override
            public T call(T v1, T v2) throws Exception {
                return (T) v1.add(v2);
            }
        });
        return encryptor.decrypt(reduce);
    }
    public void saveAsObjectFile(String path){
        rdd.saveAsObjectFile(path);
    }
}
