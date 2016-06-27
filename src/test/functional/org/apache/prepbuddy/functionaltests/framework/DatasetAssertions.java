package org.apache.prepbuddy.functionaltests.framework;

import org.apache.prepbuddy.qualityanalyzers.DataType;
import org.junit.Assert;

import java.io.Serializable;

public class DatasetAssertions extends Assert implements Serializable {

    public static void assertType(DataType expected, DataType actual) {
        assertEquals(expected, actual);
    }
}
