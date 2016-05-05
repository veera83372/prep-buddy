package org.apache.prepbuddy.utils;

import com.n1analytics.paillier.PaillierPrivateKey;
import com.n1analytics.paillier.PaillierPublicKey;

import java.io.Serializable;

public class EncryptionKeyPair implements Serializable {
    private final PaillierPrivateKey paillierPrivateKey;

    public EncryptionKeyPair(int seed) {
        paillierPrivateKey = PaillierPrivateKey.create(seed);
    }

    public String getPrivateKeyAsString() {
        return String.format("%s/%s",paillierPrivateKey.getPublicKey().getModulus(), paillierPrivateKey.getSecretValue());
    }

    public String getPublicKeyAsString() {
        return String.valueOf(paillierPrivateKey.getPublicKey().getModulus());
    }

    public PaillierPrivateKey getPrivateKey(){
        return paillierPrivateKey;
    }

    public PaillierPublicKey getPublicKey(){
        return paillierPrivateKey.getPublicKey();
    }
}
