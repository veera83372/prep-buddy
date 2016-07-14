package org.apache.datacommons.prepbuddy.api.java

import org.apache.datacommons.prepbuddy.rdds.TransformableRDD
import org.apache.spark.api.java.JavaRDD

class JavaTransformableRDD(rdd: JavaRDD[String]) extends JavaRDD[String](rdd.rdd) {
    private val tRDD: TransformableRDD = new TransformableRDD(rdd.rdd)

    def deduplicate: JavaTransformableRDD = {
        new JavaTransformableRDD(tRDD.deduplicate().toJavaRDD())
    }
}
