package org.apache.prepbuddy.utils;

import java.io.Serializable;

public interface ReplacementFunction extends Serializable {
    String replace(RowRecord record);
}
