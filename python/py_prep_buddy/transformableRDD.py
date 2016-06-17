from pyspark import RDD

from buddySerializer import BuddySerializer


class TransformableRDD(RDD):
    def __init__(self, rdd, file_type='CSV', t_rdd=None, sc=None):
        if t_rdd is None:
            jvm = rdd.ctx._jvm
            self.__set_file_type(jvm, file_type)
            self.ctx = rdd.ctx
            java_rdd = rdd._reserialize(BuddySerializer())._jrdd.map(
                    jvm.org.apache.prepbuddy.pythonConnector.BytesToString())
            self._t_rdd = jvm.org.apache.prepbuddy.rdds.TransformableRDD(java_rdd, self.__file_type)
            RDD.__init__(self, rdd._jrdd, rdd.ctx)
        else:
            jvm = sc._jvm
            self.__file_type = file_type
            self._t_rdd = t_rdd
            rdd = t_rdd.map(jvm.org.apache.prepbuddy.pythonConnector.StringToBytes())
            RDD.__init__(self, rdd, sc, BuddySerializer())

    def __set_file_type(self, jvm, file_type):
        file_types = {
            'CSV': jvm.org.apache.prepbuddy.typesystem.FileType.CSV,
            'TSV': jvm.org.apache.prepbuddy.typesystem.FileType.TSV
        }

        if file_type.upper() in file_types:
            self.__file_type = file_types[file_type.upper()]
        else:
            raise ValueError('"%s" is not a valid file type\nValid file types are CSV and TSV' % file_type)

    def deduplicate(self):
        return TransformableRDD(None, self.__file_type, self._t_rdd.deduplicate(), sc=self.ctx)
