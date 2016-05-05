package org.apache.prepbuddy.encryptors;

import com.n1analytics.paillier.PaillierContext;
import com.n1analytics.paillier.PaillierPublicKey;
import org.apache.prepbuddy.filetypes.FileType;
import org.apache.prepbuddy.utils.EncryptionKeyPair;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;

import static org.apache.commons.lang.StringUtils.isNumeric;

public class TransformableRDD extends JavaRDD  {
    private FileType fileType;

    public TransformableRDD(JavaRDD rdd, FileType fileType) {
        super(rdd.rdd(),rdd.rdd().elementClassTag());
        this.fileType = fileType;
    }

    public HomomorphicallyEncryptedRDD encryptHomomorphically(EncryptionKeyPair keyPair, int columnIndex) {
        PaillierPublicKey publicKey = keyPair.getPublicKey();
        PaillierContext signedContext = publicKey.createSignedContext();
        JavaRDD<String> map = wrapRDD(rdd()).map(new Function<String, String>() {
            @Override
            public String call(String row) throws Exception {
                String[] values = fileType.parseRecord(row.toString());
                String numericValue = values[columnIndex];
                values[columnIndex] = signedContext.encrypt(Double.parseDouble(numericValue)).toString();
                return fileType.join(values);
            }
        });
        return new HomomorphicallyEncryptedRDD(map.rdd(),keyPair,fileType);
    }
}
