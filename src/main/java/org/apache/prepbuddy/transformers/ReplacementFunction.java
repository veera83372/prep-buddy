package org.apache.prepbuddy.transformers;

import org.apache.prepbuddy.utils.RowRecord;

import java.io.Serializable;

public interface ReplacementFunction extends Serializable {
    String replace(RowRecord record);
}
