package org.apache.prepbuddy.transformations.encryption;

import com.n1analytics.paillier.EncryptedNumber;
import junit.framework.Assert;
import org.apache.prepbuddy.transformations.SparkTestCase;
import org.apache.spark.SparkException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

public class EncryptionTest extends SparkTestCase{
    @Test
    public void shouldDoEncryptionOnNumbersAndReturnBackAndEncryptedRDD() {
        JavaRDD<String> dataSet = context.parallelize(Arrays.asList("1","2","3"));
        Encryptor encryptor = new Encryptor();
        HomomorphicEncryptedRDD encryptedRDD = encryptor.encrypt(dataSet,0);
        Assert.assertEquals(6d, encryptedRDD.sum(encryptor));
    }

    @Test(expected = SparkException.class)
    public void shouldThrowExceptionIfColumnIndexIsInvalid() {
        JavaRDD<String> dataSet = context.parallelize(Arrays.asList("1", "2", "3"));
        Encryptor encryptor = new Encryptor();
        HomomorphicEncryptedRDD encryptedRDD = encryptor.encrypt(dataSet,1);
        encryptedRDD.sum(encryptor);
    }
}
