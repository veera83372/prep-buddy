package org.apache.prepbuddy.preprocessor;

public enum FileTypes{
    CSV(","),
    TSV("\t");

    final private String delimiter;

    FileTypes(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getDelimiter(){
        return delimiter;
    }
}
