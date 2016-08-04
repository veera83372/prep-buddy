package com.thoughtworks.datacommons.prepbuddy.api.java;

import com.thoughtworks.datacommons.prepbuddy.utils.RowRecord;

import java.io.Serializable;

public interface MarkerPredicate extends Serializable {
    boolean evaluate(RowRecord row);
}
