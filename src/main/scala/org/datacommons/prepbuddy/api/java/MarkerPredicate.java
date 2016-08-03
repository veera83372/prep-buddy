package org.datacommons.prepbuddy.api.java;

import org.datacommons.prepbuddy.utils.RowRecord;

import java.io.Serializable;

public interface MarkerPredicate extends Serializable {
    boolean evaluate(RowRecord row);
}
