package org.apache.prepbuddy.cluster;

import org.apache.prepbuddy.exceptions.SystemException;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * TextFacets contains the cardinal values of the column with their
 * number of occurrence in that column.
 */
public class TextFacets implements Serializable {
    private final JavaPairRDD<String, Integer> facets;

    public TextFacets(JavaPairRDD<String, Integer> facets) {
        this.facets = facets;
    }

    public long count() {
        return facets.count();
    }

    public List<Tuple2> highest() {
        return getPeakListFor(new Function2<Integer, Integer, Boolean>() {
            @Override
            public Boolean call(Integer currentTupleValue, Integer peakTupleValue) throws Exception {
                return currentTupleValue > peakTupleValue;
            }
        });
    }

    public List<Tuple2> lowest() throws Exception {
        return getPeakListFor(new Function2<Integer, Integer, Boolean>() {
            @Override
            public Boolean call(Integer currentTupleValue, Integer peakTupleValue) throws Exception {
                return currentTupleValue < peakTupleValue;
            }
        });
    }

    private List<Tuple2> getPeakListFor(Function2<Integer, Integer, Boolean> compareFunction) {
        List<Tuple2<String, Integer>> allTuple = facets.collect();
        ArrayList<Tuple2> list = new ArrayList<>();
        try {
            Tuple2<String, Integer> peakTuple = allTuple.get(0);
            list.add(peakTuple);
            for (Tuple2<String, Integer> tuple : allTuple) {
                if (compareFunction.call(tuple._2(), peakTuple._2())) {
                    peakTuple = tuple;
                    list.clear();
                    list.add(peakTuple);
                }

                if (tuple._2().equals(peakTuple._2()) && !(tuple.equals(peakTuple))) {
                    list.add(tuple);
                }

            }
        } catch (Exception e) {
            throw new SystemException(e);
        }
        return list;
    }


    public JavaPairRDD<String, Integer> rdd() {
        return facets;
    }

    public List<Tuple2> getFacetsBetween(int lowerBound, int upperBound) {
        List<Tuple2<String, Integer>> allTuple = facets.collect();
        ArrayList<Tuple2> list = new ArrayList<>();

        for (Tuple2<String, Integer> tuple : allTuple) {
            Integer currentTupleValue = tuple._2();

            if (isInRange(currentTupleValue, lowerBound, upperBound))
                list.add(tuple);
        }
        return list;
    }

    private boolean isInRange(Integer currentTupleValue, int minimum, int maximum) {
        return currentTupleValue >= minimum && currentTupleValue <= maximum;
    }


    public List<String> cardinalValues() {
        List<Tuple2<String, Integer>> tuples = facets.collect();
        ArrayList<String> keys = new ArrayList<>();
        for (Tuple2<String, Integer> tuple : tuples) {
            keys.add(tuple._1());
        }
        return keys;
    }
}

