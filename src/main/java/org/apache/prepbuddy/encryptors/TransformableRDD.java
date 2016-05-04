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

public class TransformableRDD<T> extends JavaRDD<T>  {
    private FileType fileType;

    public TransformableRDD(JavaRDD<T> rdd, FileType fileType) {
        super(rdd.rdd(),rdd.rdd().elementClassTag());
        this.fileType = fileType;
    }

    public HomomorphicallyEncryptedRDD encryptHomomorphically(EncryptionKeyPair keyPair, int columnIndex) {
        PaillierPublicKey publicKey = new PaillierPublicKey(new BigInteger(keyPair.getPublicKeyAsString()));
        PaillierContext signedContext = publicKey.createSignedContext();
        JavaRDD<List> map = wrapRDD(rdd()).map(new Function<T, List>() {
            @Override
            public List call(T value) throws Exception {
                String[] values = fileType.parseRecord(value.toString());
                String numericValue = values[columnIndex].split("\\.",2)[0];
                List <Object> list = new ArrayList<Object>(values.length);
                for (String s : values)  list.add(s);
                if (isNumeric(numericValue)) {
                    list.remove(columnIndex);
                    list.add(columnIndex, signedContext.encrypt(Double.parseDouble(numericValue)));
                }
                return list;
            }
        });
        return new HomomorphicallyEncryptedRDD(map.rdd(),map.classTag(),keyPair,fileType);
    }
}
