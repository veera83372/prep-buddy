from py4j.java_gateway import java_import
from pyspark import RDD


class TransformableRDD(RDD):
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

    def __init__(self, rdd, file_type='CSV', t_rdd=None, sc=None):
        if t_rdd == None:
            jvm = rdd.ctx._jvm

            # java_import(jvm, 'org.apache.prepbuddy.*')
            # rdd._jrdd.mapToPair(jvm.BytesToKeyAndSeries())
            self.__set_file_type(jvm, file_type)
            java_import(jvm, 'org.apache.prepbuddy.rdds.*')
            self._trdd = jvm.TransformableRDD(rdd._jrdd, self.__file_type)
            RDD.__init__(self, rdd._jrdd, rdd.ctx)

        else:
            jvm = sc._jvm
            self.__file_type = file_type
            # java_import(jvm, 'org.apache.prepbuddy.rdds.*')
            # trdd = jvm.TransformableRDD(t_rdd._jrdd, self.__file_type)
            java_import(jvm, 'org.apache.prepbuddy.pythonConnector.*')
            self._trdd = t_rdd.map(jvm.org.apache.prepbuddy.KeyAndSeriesToBytes())
            # RDD.__init__(self, jrdd, sc, _TimeSeriesSerializer())
            RDD.__init__(self, self._trdd, sc)

    def deduplicate(self):

        return TransformableRDD(None, self.__file_type, self._trdd.deduplicate(), sc=self.ctx)

    def select(self, columnIndex):
        return self._trdd.select(columnIndex)
