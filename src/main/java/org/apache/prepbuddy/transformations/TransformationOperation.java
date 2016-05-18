package org.apache.prepbuddy.transformations;

import java.io.Serializable;

public interface TransformationOperation extends Serializable{
    String[] apply(String[] rowRecord);
}
