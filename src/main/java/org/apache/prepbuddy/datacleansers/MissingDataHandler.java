package org.apache.prepbuddy.datacleansers;

import org.apache.prepbuddy.utils.RowRecord;

import java.io.Serializable;

public interface MissingDataHandler extends Serializable{
    String handleMissingData(RowRecord record) ;
}
