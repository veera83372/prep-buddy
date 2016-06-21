from pyspark import RDD

from serializer import BuddySerializer
from py_prep_buddy.cluster.clusters import Clusters
from py_prep_buddy.cluster.text_facets import TextFacets


class TransformableRDD(RDD):
    def __init__(self, rdd, file_type='CSV', t_rdd=None, sc=None):
        if rdd is not None:
            jvm = rdd.ctx._jvm
            self.__set_file_type(jvm, file_type)
            self.spark_context = rdd.ctx
            java_rdd = rdd._reserialize(BuddySerializer())._jrdd.map(
                    jvm.org.apache.prepbuddy.python.connector.BytesToString())
            self._transformable_rdd = jvm.org.apache.prepbuddy.rdds.TransformableRDD(java_rdd, self.__file_type)
            RDD.__init__(self, rdd._jrdd, rdd.ctx)
        else:
            jvm = sc._jvm
            self.__set_file_type(jvm, file_type)
            self._transformable_rdd = t_rdd
            rdd = t_rdd.map(jvm.org.apache.prepbuddy.python.connector.StringToBytes())
            RDD.__init__(self, rdd, sc, BuddySerializer())

    def __set_file_type(self, jvm, file_type):
        file_types = {
            'CSV': jvm.org.apache.prepbuddy.qualityanalyzers.FileType.CSV,
            'TSV': jvm.org.apache.prepbuddy.qualityanalyzers.FileType.TSV
        }
        if file_type in file_types.values():
            self.__file_type = file_type
        elif file_type.upper() in file_types:
            self.__file_type = file_types[file_type.upper()]
        else:
            raise ValueError('"%s" is not a valid file type\nValid file types are CSV and TSV' % file_type)

    def deduplicate(self):
        return TransformableRDD(None, self.__file_type, self._transformable_rdd.deduplicate(), sc=self.spark_context)

    def impute(self, column_index, imputation_strategy):
        strategy_apply = imputation_strategy.get_strategy(self.spark_context)
        return TransformableRDD(None,
                                self.__file_type,
                                self._transformable_rdd.impute(column_index, strategy_apply),
                                sc=self.spark_context)

    def clusters(self, column_index, clusteringAlgorithm):
        algorithm = clusteringAlgorithm.get_algorithm(self.spark_context)
        return Clusters(self._transformable_rdd.clusters(column_index, algorithm))

    def listFacets(self, column_index):
        return TextFacets(self._transformable_rdd.listFacets(column_index))


