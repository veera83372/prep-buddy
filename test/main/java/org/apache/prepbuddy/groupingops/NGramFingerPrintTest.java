package org.apache.prepbuddy.groupingops;

import org.apache.prepbuddy.SparkTestCase;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;

public class NGramFingerprintTest extends SparkTestCase {
    @Test
    public void generateNGramFingerprintShouldGive_arispari_Of_ParisIn2Gram() {
        String parisNGramFingerPrint = new NGramFingerprint(2).generateNGramFingerprint("Paris");
        String expected ="arispari";
        assertEquals(expected, parisNGramFingerPrint);
    }
    @Test
    public void generateNGramFingerprintShouldGive_aiprs_Of_ParisIn2Gram() {
        String parisNGramFingerPrint = new NGramFingerprint(1).generateNGramFingerprint("Paris");
        String expected ="aiprs";
        assertEquals(expected, parisNGramFingerPrint);
    }

    @Test
    public void shouldGiveSameKeyForTwoDifferentStringBtHaveSameAlphabets() {
        String firstKey = new NGramFingerprint(1).generateNGramFingerprint("qwertyuiop");
        String secondKey = new NGramFingerprint(1).generateNGramFingerprint("poiuytrewQ");
        assertEquals(firstKey, secondKey);
    }

}
