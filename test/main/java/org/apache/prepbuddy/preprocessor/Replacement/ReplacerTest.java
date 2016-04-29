package org.apache.prepbuddy.preprocessor.Replacement;

import org.apache.prepbuddy.transformations.SparkTestCase;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ReplacerTest extends SparkTestCase {

    @Test
    public void shouldCallbackTheMissingDataHandler() {
        Replacer replacer = new Replacer(",");
        replacer.add(new ReplaceHandler(0, (value) -> "10"));
        replacer.add(new ReplaceHandler(3, (value) -> "50"));


        String expected = "10,2,3,50";
        String actual = replacer.apply("1,2,3,4");
        assertEquals(expected, actual);
    }

}