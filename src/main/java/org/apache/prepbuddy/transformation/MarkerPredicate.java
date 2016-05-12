package org.apache.prepbuddy.transformation;

import org.apache.prepbuddy.utils.RowRecord;

import java.io.Serializable;

public interface MarkerPredicate extends Serializable {
       boolean evaluate(RowRecord row);
}
