package com.thoughtworks.datacommons.prepbuddy.api.java;

import com.thoughtworks.datacommons.prepbuddy.utils.RowRecord;

import java.io.Serializable;

public interface RowPurger extends Serializable {
    Boolean evaluate(RowRecord record);
}
