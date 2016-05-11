package org.apache.prepbuddy.transformation;

import org.apache.prepbuddy.coreops.RowTransformation;
import org.apache.prepbuddy.filetypes.FileType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.io.Serializable;

public class MapByMark implements RowTransformation, Serializable {
    private final String symbol;
    private final Function mapFunction;
    private final int columnIndex;

    public MapByMark(String symbol, int columnIndex , Function mapFunction) {
        this.symbol = symbol;
        this.mapFunction = mapFunction;
        this.columnIndex = columnIndex;
    }

    @Override
    public JavaRDD<String> apply(JavaRDD<String> dataset, FileType type) {
        return dataset.map(new Function<String, String>() {
            @Override
            public String call(String row) throws Exception {
                String[] records = type.parseRecord(row);
                String lastColumn = records[columnIndex];
                return lastColumn.equals(symbol) ? (String) mapFunction.call(row) : row;
            }
        });
    }
}
