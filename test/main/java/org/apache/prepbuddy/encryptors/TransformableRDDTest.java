package org.apache.prepbuddy.encryptors;

import org.apache.prepbuddy.SparkTestCase;
import org.apache.prepbuddy.filetypes.FileType;
import org.apache.prepbuddy.utils.EncryptionKeyPair;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Assert;
import org.junit.Test;

import java.math.BigInteger;
import java.util.Arrays;

public class TransformableRDDTest extends SparkTestCase {
    @Test
    public void shouldEncryptAColumn() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("1,X","2,Y","3,Z","4,A"));
        TransformableRDD transformableRDD = new TransformableRDD(initialDataset, FileType.CSV);
        EncryptionKeyPair keyPair = new  EncryptionKeyPair(1024);
        HomomorphicallyEncryptedRDD encryptedRDD = transformableRDD.encryptHomomorphically(keyPair,0);
        JavaRDD<String> dataSet = encryptedRDD.decrypt(0);
        Assert.assertEquals(dataSet.collect(),initialDataset.collect());
    }

    @Test
    public void shouldBeAbleToGetSumOfTheEncryptedColumn() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("5,X","2,Y","13,Z","4,A"));
        TransformableRDD transformableRDD = new TransformableRDD(initialDataset, FileType.CSV);
        EncryptionKeyPair keyPair = new  EncryptionKeyPair(1024);
        HomomorphicallyEncryptedRDD encryptedRDD = transformableRDD.encryptHomomorphically(keyPair,0);
        BigInteger sum = encryptedRDD.sum(0);

        Assert.assertEquals(sum,new BigInteger("24"));
    }

    @Test
    public void shouldBeAbleToGetAverageOfTheEncryptedColumn() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("X,3,s","1,2,Y","Z,1,p","A,1,N"));
        TransformableRDD transformableRDD = new TransformableRDD(initialDataset, FileType.CSV);
        EncryptionKeyPair keyPair = new  EncryptionKeyPair(1024);
        HomomorphicallyEncryptedRDD encryptedRDD = transformableRDD.encryptHomomorphically(keyPair,1);
        double average = encryptedRDD.average(1);

        Assert.assertEquals(average,1.75,0.01);
    }

    @Test
    public void shouldBeAbleToGetSumOfTheEncryptedColumnForDouble() {
        JavaRDD<String> initialDataset = javaSparkContext.parallelize(Arrays.asList("3.1,X","2.1,Y","13.1,Z","4.1,A"));
        TransformableRDD transformableRDD = new TransformableRDD(initialDataset, FileType.CSV);
        EncryptionKeyPair keyPair = new  EncryptionKeyPair(1024);
        HomomorphicallyEncryptedRDD encryptedRDD = transformableRDD.encryptHomomorphically(keyPair,0);
        BigInteger sum = encryptedRDD.sum(0);

        Assert.assertEquals(sum,new BigInteger("22"));
    }
}
