package org.apache.prepbuddy.utils;

import com.n1analytics.paillier.PaillierPrivateKey;
import com.n1analytics.paillier.PaillierPublicKey;

import java.io.Serializable;

public class EncryptionKeyPair implements Serializable {
    private final PaillierPrivateKey paillierPrivateKey;

    public EncryptionKeyPair(int seed) {
        paillierPrivateKey = PaillierPrivateKey.create(seed);
    }

    public PaillierPrivateKey getPrivateKey(){
        return paillierPrivateKey;
    }

    public PaillierPublicKey getPublicKey(){
        return paillierPrivateKey.getPublicKey();
    }
}
