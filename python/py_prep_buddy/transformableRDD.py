from py4j.java_gateway import java_import
from pyspark import RDD


class TransformableRDD(RDD):
    def __init__(self, rdd, filetype=None):
        jvm = rdd.ctx._jvm
        java_import(jvm, "org.apache.prepbuddy.rdds.*")
        trdd = jvm.TransformableRDD(rdd._jrdd)
        self._trdd = trdd
        self._filetype = filetype
        RDD.__init__(self, rdd._jrdd, rdd._jrdd)

    def deduplicate(self):
        return self._trdd.deduplicate()
