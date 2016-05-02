package org.apache.prepbuddy.reformatters;

import java.io.Serializable;

public interface TransformationFunction extends Serializable {
    String apply(String existingValue, String[] row);
}
