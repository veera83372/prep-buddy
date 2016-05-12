package org.apache.prepbuddy.transformation;


public class SplitByDelimiter extends ColumnSplitter {
    private String separator;
    private Integer maxPartition;

    public SplitByDelimiter(String separator, Integer maxPartition, boolean retainColumn) {
        super(retainColumn);
        this.separator = separator;
        this.maxPartition = maxPartition;
    }

    public SplitByDelimiter(String separator, boolean retainColumn) {
        this(separator, null, retainColumn);
    }

    @Override
    String[] splitColumn(String columnValue) {
        if (maxPartition == null)
            return columnValue.split(separator);
        return columnValue.split(separator, maxPartition);
    }
}
