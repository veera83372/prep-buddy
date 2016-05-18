package org.apache.prepbuddy.transformations;

import org.apache.prepbuddy.utils.RowRecord;

import java.io.Serializable;

public interface MarkerPredicate extends Serializable {
       boolean evaluate(RowRecord row);
}
