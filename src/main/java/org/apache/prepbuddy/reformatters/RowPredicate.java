package org.apache.prepbuddy.reformatters;

import java.io.Serializable;

public interface RowPredicate extends Serializable{
    Boolean evaluate(String record);
}
