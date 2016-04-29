package org.apache.prepbuddy.transformations.encryption;

import com.n1analytics.paillier.EncryptedNumber;
import com.n1analytics.paillier.PaillierContext;
import com.n1analytics.paillier.PaillierPrivateKey;
import com.n1analytics.paillier.PaillierPublicKey;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

import static org.apache.commons.lang.StringUtils.*;

public class Encryptor implements Serializable {
    private PaillierPrivateKey privateKey = PaillierPrivateKey.create(1024);;
    private PaillierPublicKey publicKey = privateKey.getPublicKey();
    private PaillierContext signedContext = publicKey.createSignedContext();

    HomomorphicEncryptedRDD encrypt(final JavaRDD dataSet, int column){
        JavaRDD encryptedRDD = dataSet.map(new Function<String ,EncryptedNumber>() {
            @Override
            public EncryptedNumber call(String record) throws Exception {
                String[] split = record.split(",");
                String value = split[column];
                if(isNumeric(value)) return signedContext.encrypt(Double.parseDouble(value));
                return null;
            }
        });
        return new HomomorphicEncryptedRDD(encryptedRDD);
    }

    public EncryptedNumber encrypt(Integer integer){
        return signedContext.encrypt(integer);
    }

    public double decrypt(EncryptedNumber encryptedNumber){
        return privateKey.decrypt(encryptedNumber).decodeDouble();
    }
}
