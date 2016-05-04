package org.apache.prepbuddy.facets;

import org.apache.prepbuddy.filetypes.FileType;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.io.Serializable;

public class FacetUtils implements Serializable {

    public TextFacets listTextFacets(JavaRDD<String> initialDataset, int columnIndex, FileType fileType) {
        JavaPairRDD<String, Integer> columnValuePair = initialDataset.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String record) throws Exception {
                String[] columnValues = fileType.parseRecord(record);
                return new Tuple2<String, Integer>(columnValues[columnIndex], 1);
            }
        });
        JavaPairRDD<String, Integer> facets = columnValuePair.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer accumulator, Integer currentValue) throws Exception {
                return accumulator + currentValue;
            }
        });
        return new TextFacets(facets);
    }


}
