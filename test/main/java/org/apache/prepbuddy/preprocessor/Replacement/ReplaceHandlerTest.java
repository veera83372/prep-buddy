package org.apache.prepbuddy.preprocessor.replacement;

import org.junit.Assert;
import org.junit.Test;

public class ReplaceHandlerTest {
    @Test
    public void shouldGiveAnArrayByReplacingTheRequiredFieldByUsingTheCallbackFunction() {
        ReplaceHandler replaceHandler = new ReplaceHandler(0, (Replacer.ReplaceFunction<String, String>) value -> "Smith");

        String[] actual = replaceHandler.replace("Some User,India".split(","));
        String[] expected = "Smith,India".split(",");

        Assert.assertArrayEquals(expected, actual); ;
    }
}