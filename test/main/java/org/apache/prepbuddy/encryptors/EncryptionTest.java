package org.apache.prepbuddy.encryptors;

import com.n1analytics.paillier.EncryptedNumber;
import junit.framework.Assert;
import org.apache.prepbuddy.SparkTestCase;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.junit.Test;

import java.util.Arrays;

public class EncryptionTest extends SparkTestCase{
    @Test
    public void shouldDoEncryptionOnNumbersAndReturnBackAndEncryptedRDD() {
        JavaRDD<String> dataSet = context.parallelize(Arrays.asList("1","2","3"));
        Encryptor encryptor = new Encryptor();
        HomomorphicEncryptedRDD encryptedRDD = encryptor.encrypt(dataSet,0);
        EncryptedNumber actual = encryptedRDD.reduce(new Function2<EncryptedNumber, EncryptedNumber, EncryptedNumber>() {
            @Override
            public EncryptedNumber call(EncryptedNumber first, EncryptedNumber second) throws Exception {
               return first.add(second);
            }
        });
        Assert.assertEquals(6d, encryptor.decrypt(actual));
    }

    @Test(expected = SparkException.class)
    public void shouldThrowExceptionIfColumnIndexIsInvalid() {
        JavaRDD<String> dataSet = context.parallelize(Arrays.asList("1", "2", "3"));
        Encryptor encryptor = new Encryptor();
        HomomorphicEncryptedRDD encryptedRDD = encryptor.encrypt(dataSet,1);
        EncryptedNumber actual = encryptedRDD.reduce(new Function2<EncryptedNumber, EncryptedNumber, EncryptedNumber>() {
            @Override
            public EncryptedNumber call(EncryptedNumber first, EncryptedNumber second) throws Exception {
                return first.add(second);
            }
        });
    }
}
