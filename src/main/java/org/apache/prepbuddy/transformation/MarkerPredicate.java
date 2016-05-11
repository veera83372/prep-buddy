package org.apache.prepbuddy.transformation;

import java.io.Serializable;

public interface MarkerPredicate extends Serializable {
       boolean evaluate(String row);
}
