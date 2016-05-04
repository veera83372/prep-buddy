package org.apache.prepbuddy.encryptors;

import com.n1analytics.paillier.EncryptedNumber;
import com.n1analytics.paillier.PaillierPrivateKey;
import org.apache.commons.lang.StringUtils;
import org.apache.prepbuddy.filetypes.FileType;
import org.apache.prepbuddy.utils.EncryptionKeyPair;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.rdd.RDD;
import scala.reflect.ClassTag;

import java.math.BigInteger;
import java.util.List;

public class HomomorphicallyEncryptedRDD<T> extends JavaRDD<T>  {
    private final EncryptionKeyPair keyPair;
    private final FileType fileType;

    protected HomomorphicallyEncryptedRDD(RDD<T> rdd, ClassTag<T> classTag, EncryptionKeyPair keyPair, FileType fileType) {
        super(rdd, classTag);
        this.keyPair = keyPair;
        this.fileType = fileType;
    }

    public JavaRDD<Object> decrypt(int columnIndex) {
        PaillierPrivateKey privateKey = keyPair.getPrivateKey();
        JavaRDD<Object> javaRDD = wrapRDD(rdd()).map(new Function<T, Object>() {
            @Override
            public Object call(T value) throws Exception {
                List list = (List) value;
                EncryptedNumber encryptedNumber = (EncryptedNumber) list.get(columnIndex);
                BigInteger bigInteger = privateKey.decrypt(encryptedNumber).decodeBigInteger();
                list.remove(columnIndex);
                list.add(columnIndex,bigInteger);
                return StringUtils.join(list,fileType.getDelimiter());
            }
        });
        return javaRDD;
    }

    public BigInteger sum(int columnIndex) {
        T reduce = wrapRDD(rdd()).reduce(new Function2<T, T, T>() {
            @Override
            public T call(T firstValue, T secondValue) throws Exception {
                List firstList = (List) firstValue;
                List secondList = (List) secondValue;
                //TODO exceptionHandling for Casting
                EncryptedNumber firstNumber = (EncryptedNumber) firstList.get(columnIndex);
                EncryptedNumber secondNumber = (EncryptedNumber)secondList.get(columnIndex);
                secondList.remove(columnIndex);
                secondList.add(columnIndex,firstNumber.add(secondNumber));
                return (T)secondList;
            }
        });
        Object value = ((List) reduce).get(columnIndex);
        EncryptedNumber result = (EncryptedNumber)value;
        return result.decrypt(keyPair.getPrivateKey()).decodeApproximateBigInteger();
    }

    public double average(int columnIndex) {
        BigInteger sum = sum(columnIndex);
        long count = rdd().count();
        return  sum.doubleValue() / count;
    }
}
