package org.apache.prepbuddy.transformation;

import java.io.Serializable;

public interface TransformationOperation extends Serializable{
    String[] apply(String[] rowRecord);
}
