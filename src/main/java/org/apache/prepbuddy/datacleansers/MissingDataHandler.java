package org.apache.prepbuddy.datacleansers;

import java.io.Serializable;

public interface MissingDataHandler extends Serializable{
    String handleMissingData(String[] record) ;
}
