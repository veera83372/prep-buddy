from py4j.java_gateway import java_import
from pyspark import RDD

from buddySerializer import BuddySerializer


class TransformableRDD(RDD):
    def __init__(self, rdd, file_type='CSV', t_rdd=None, sc=None):
        if t_rdd is None:
            jvm = rdd.ctx._jvm
            self.__set_file_type(jvm, file_type)
            self.ctx = rdd.ctx
            java_import(jvm, 'org.apache.prepbuddy.rdds.TransformableRDD')
            java_import(jvm, 'org.apache.prepbuddy.pythonConnector.*')
            java_rdd = rdd._reserialize(BuddySerializer())._jrdd.map(jvm.BytesToString())
            self._t_rdd = jvm.TransformableRDD(java_rdd, self.__file_type)
            RDD.__init__(self, rdd._jrdd, rdd.ctx)
        else:
            jvm = sc._jvm
            self.__file_type = file_type
            java_import(jvm, 'org.apache.prepbuddy.pythonConnector.*')
            self._t_rdd = t_rdd
            rdd = t_rdd.map(jvm.StringToBytes())
            RDD.__init__(self, rdd, sc, BuddySerializer())

    def __set_file_type(self, jvm, file_type):
        java_import(jvm, "org.apache.prepbuddy.typesystem.*")
        file_types = {
            'CSV': jvm.FileType.CSV,
            'TSV': jvm.FileType.TSV
        }

        if file_types.has_key(file_type.upper()):
            self.__file_type = file_types[file_type.upper()]
        else:
            raise ValueError('"%s" is not a valid file type\nValid file types are CSV and TSV' % file_type)

    def deduplicate(self):
        return TransformableRDD(None, self.__file_type, self._t_rdd.deduplicate(), sc=self.ctx)
