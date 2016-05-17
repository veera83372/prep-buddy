package org.apache.prepbuddy.groupingops;

import scala.Tuple2;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class Cluster implements Serializable{

    private final String key;
    private List<Tuple2> tuples = new ArrayList<>();

    public Cluster(String key) {
        this.key = key;
    }

    public void add(Tuple2<String, Integer> recordTuple) {
        tuples.add(recordTuple);
    }

    public boolean contain(Tuple2<String, Integer> otherTuple) {
        return tuples.contains(otherTuple);
    }

    public int size() {
        return tuples.size();
    }

    public boolean isOfKey(String key) {
        return this.key.equals(key);
    }

    public boolean containValue(String value) {
        for (Tuple2 tuple : tuples) {
            if (tuple._1().equals(value))
                return true;
        }
        return false;
    }
}
