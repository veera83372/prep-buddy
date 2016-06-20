package org.apache.prepbuddy.cluster;

import org.apache.prepbuddy.SparkTestCase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class NGramFingerprintAlgorithmTest extends SparkTestCase {
    @Test
    public void generateNGramFingerprintShouldGive_arispari_Of_ParisIn2Gram() {
        String parisNGramFingerPrint = new NGramFingerprintAlgorithm(2).generateNGramFingerprint("Paris");
        String expected = "arispari";
        assertEquals(expected, parisNGramFingerPrint);
    }

    @Test
    public void generateNGramFingerprintShouldGive_aiprs_Of_ParisIn2Gram() {
        String parisNGramFingerPrint = new NGramFingerprintAlgorithm(1).generateNGramFingerprint("Paris");
        String expected = "aiprs";
        assertEquals(expected, parisNGramFingerPrint);
    }

    @Test
    public void shouldGiveSameKeyForTwoDifferentStringBtHaveSameAlphabets() {
        String firstKey = new NGramFingerprintAlgorithm(1).generateNGramFingerprint("qwertyuiop");
        String secondKey = new NGramFingerprintAlgorithm(1).generateNGramFingerprint("poiuytrewQ");
        assertEquals(firstKey, secondKey);
    }

}
