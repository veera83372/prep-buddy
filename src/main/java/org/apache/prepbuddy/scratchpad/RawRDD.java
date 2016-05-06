package org.apache.prepbuddy.scratchpad;

import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import scala.collection.Iterator;
import scala.collection.Seq;
import scala.reflect.ClassTag;

public class RawRDD<T> extends RDD{
    private RDD<?> parent;

    public RawRDD(SparkContext _sc, Seq<Dependency<?>> deps, ClassTag evidence$1) {
        super(_sc, deps, evidence$1);
    }

    public RawRDD(RDD<?> oneParent, ClassTag evidence$2) {
        super(oneParent, evidence$2);
    }
    @Override
    public Iterator compute(Partition split, TaskContext context) {
        return parent.iterator(split, context);
    }

    @Override
    public Partition[] getPartitions() {
        return parent.partitions();
    }
}

