package org.apache.prepbuddy.encryptors;

import com.n1analytics.paillier.EncryptedNumber;
import com.n1analytics.paillier.PaillierPrivateKey;
import org.apache.prepbuddy.typesystem.FileType;
import org.apache.prepbuddy.utils.EncryptionKeyPair;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.rdd.RDD;

import java.math.BigInteger;

public class HomomorphicallyEncryptedRDD extends JavaRDD<String>  {
    private final EncryptionKeyPair keyPair;
    private final FileType fileType;

    public HomomorphicallyEncryptedRDD(RDD rdd,EncryptionKeyPair keyPair, FileType fileType) {
        super(rdd, rdd.elementClassTag());
        this.keyPair = keyPair;
        this.fileType = fileType;
    }

    public JavaRDD<String> decrypt(final int columnIndex) {
        final PaillierPrivateKey privateKey = keyPair.getPrivateKey();
        JavaRDD<String> javaRDD = wrapRDD(rdd()).map(new Function<String, String>() {
            @Override
            public String call(String row) throws Exception {
                String[] values = fileType.parseRecord(row);
                EncryptedNumber encryptedNumber = EncryptedNumber.create(values[columnIndex],keyPair.getPrivateKey());
                BigInteger bigInteger = privateKey.decrypt(encryptedNumber).decodeApproximateBigInteger();
                values[columnIndex] = bigInteger.toString();
                return fileType.join(values);
            }
        });
        return javaRDD;
    }

    public BigInteger sum(final int columnIndex) {
        String finalRecord = wrapRDD(rdd()).reduce(new Function2<String, String, String>() {
            @Override
            public String call(String firstRow, String secondRow) throws Exception {
                String[] firstRecord = fileType.parseRecord(firstRow);
                String[] secondRecord = fileType.parseRecord(secondRow);
                EncryptedNumber firstNumber = EncryptedNumber.create(firstRecord[columnIndex], keyPair.getPrivateKey());
                EncryptedNumber secondNumber = EncryptedNumber.create(secondRecord[columnIndex], keyPair.getPrivateKey());
                firstRecord[columnIndex] = firstNumber.add(secondNumber).toString();
                return fileType.join(firstRecord);
            }
        });
        String sum = fileType.parseRecord(finalRecord)[columnIndex];
        EncryptedNumber result = EncryptedNumber.create(sum, keyPair.getPrivateKey());
        return result.decrypt(keyPair.getPrivateKey()).decodeApproximateBigInteger();
    }

    public double average(int columnIndex) {
        BigInteger sum = sum(columnIndex);
        long count = rdd().count();
        return  sum.doubleValue() / count;
    }
}
