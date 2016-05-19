package org.apache.prepbuddy.transformations;


public class SplitByDelimiter extends SplitPlan {
    private String separator;
    private Integer maxPartition;

    public SplitByDelimiter(String separator, boolean retainColumn, Integer maxPartition) {
        super(retainColumn);
        this.separator = separator;
        this.maxPartition = maxPartition;
    }

    public SplitByDelimiter(String separator, boolean retainColumn) {
        this(separator, retainColumn, null);
    }

    @Override
    String[] splitColumn(String columnValue) {
        if (maxPartition == null)
            return columnValue.split(separator);
        return columnValue.split(separator, maxPartition);
    }
}
