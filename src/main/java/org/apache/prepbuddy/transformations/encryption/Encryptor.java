package org.apache.prepbuddy.transformations.encryption;

import com.n1analytics.paillier.EncryptedNumber;
import com.n1analytics.paillier.PaillierContext;
import com.n1analytics.paillier.PaillierPrivateKey;
import com.n1analytics.paillier.PaillierPublicKey;
import org.apache.prepbuddy.transformations.imputation.ColumnIndexOutOfBoundsException;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

import static org.apache.commons.lang.StringUtils.*;

public class Encryptor implements Serializable {
    private PaillierPrivateKey privateKey = PaillierPrivateKey.create(1024);;
    private PaillierPublicKey publicKey = privateKey.getPublicKey();
    private PaillierContext signedContext = publicKey.createSignedContext();

    HomomorphicEncryptedRDD encrypt(final JavaRDD dataSet, int columnIndex){
        JavaRDD encryptedRDD = dataSet.map(new Function<String ,EncryptedNumber>() {
            @Override
            public EncryptedNumber call(String record) throws Exception {
                String[] columnValues = record.split(",");
                if (columnIndex >= columnValues.length || columnIndex < 0)
                    throw new ColumnIndexOutOfBoundsException("No column found on index:: " + columnIndex);
                String value = columnValues[columnIndex];
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
