package org.apache.prepbuddy.groupingops;

import org.apache.prepbuddy.SparkTestCase;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class NGramFingerPrintTest extends SparkTestCase {
    @Test
    public void generateNGramFingerprintShouldGive_arispari_Of_ParisIn2Gram() {
        String parisNGramFingerPrint = FingerprintingAlgorithms.generateNGramFingerprint("Paris", 2);
        String expected ="arispari";
        assertEquals(expected, parisNGramFingerPrint);
    }
    @Test
    public void generateNGramFingerprintShouldGive_aiprs_Of_ParisIn2Gram() {
        String parisNGramFingerPrint = FingerprintingAlgorithms.generateNGramFingerprint("Paris", 1);
        String expected ="aiprs";
        assertEquals(expected, parisNGramFingerPrint);
    }

    @Test
    public void shouldGiveSameKeyForTwoDifferentStringBtHaveSameAlphabets() {
        String firstKey = FingerprintingAlgorithms.generateNGramFingerprint("qwertyuiop", 1);
        String secondKey = FingerprintingAlgorithms.generateNGramFingerprint("poiuytrewQ", 1);
        assertEquals(firstKey, secondKey);
    }

}
