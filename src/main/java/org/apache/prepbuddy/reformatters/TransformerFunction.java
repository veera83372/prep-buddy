package org.apache.prepbuddy.reformatters;

import java.io.Serializable;

public interface TransformerFunction extends Serializable {
    String apply(String input);
}
