package org.apache.prepbuddy.coreops;

import java.io.Serializable;

public interface TransformationFunction extends Serializable {
    String apply(String existingValue, String[] row);
}
