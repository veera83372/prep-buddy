package org.apache.prepbuddy.datacleansers.imputation;

import org.apache.prepbuddy.groupingops.TextFacets;
import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.utils.RowRecord;
import scala.Tuple2;

import java.util.List;

public class ModeSubstitution implements ImputationStrategy {
    private  Tuple2 highest;
    @Override
    public void prepareSubstitute(TransformableRDD rdd, int missingDataColumn) {
        TextFacets textFacets = rdd.listFacets(missingDataColumn);
        List<Tuple2> listOfHighest = textFacets.highest();
        highest = listOfHighest.get(0);
    }

    @Override
    public String handleMissingData(RowRecord record) {
        return highest._1().toString();
    }
}
