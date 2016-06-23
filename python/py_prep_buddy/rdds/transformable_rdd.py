from pyspark import RDD
from py4j.java_gateway import java_import

from py_prep_buddy import py2java_int_array
from py_prep_buddy.class_names import ClassNames
from py_prep_buddy.serializer import BuddySerializer
from py_prep_buddy.cluster.clusters import Clusters
from py_prep_buddy.cluster.text_facets import TextFacets


class TransformableRDD(RDD):
    def __init__(self, rdd, file_type='CSV', t_rdd=None, sc=None):
        if rdd is not None:
            jvm = rdd.ctx._jvm
            java_import(jvm, ClassNames.BYTES_TO_STRING)
            java_import(jvm, ClassNames.TRANSFORMABLE_RDD)

            self.__set_file_type(jvm, file_type)
            self.spark_context = rdd.ctx
            java_rdd = rdd._reserialize(BuddySerializer())._jrdd.map(
                    jvm.BytesToString())
            self._transformable_rdd = jvm.TransformableRDD(java_rdd, self.__file_type)
            RDD.__init__(self, rdd._jrdd, rdd.ctx)
        else:
            jvm = sc._jvm
            java_import(jvm, ClassNames.STRING_TO_BYTES)
            self.__set_file_type(jvm, file_type)
            self._transformable_rdd = t_rdd
            rdd = t_rdd.map(jvm.StringToBytes())
            RDD.__init__(self, rdd, sc, BuddySerializer())

    def __set_file_type(self, jvm, file_type):
        java_import(jvm, ClassNames.FileType)
        file_types = {
            'CSV': jvm.FileType.CSV,
            'TSV': jvm.FileType.TSV
        }
        if file_type in file_types.values():
            self.__file_type = file_type
        elif file_type.upper() in file_types:
            self.__file_type = file_types[file_type.upper()]
        else:
            raise ValueError('"%s" is not a valid file type\nValid file types are CSV and TSV' % file_type)

    def deduplicate(self):
        return TransformableRDD(None, self.__file_type, self._transformable_rdd.deduplicate(), sc=self.spark_context)

    def deduplicate(self, column_indexes):
        return TransformableRDD(None, self.__file_type, self._transformable_rdd.deduplicate(column_indexes), sc=self.spark_context)

    def impute(self, column_index, imputation_strategy):
        strategy_apply = imputation_strategy.get_strategy(self.spark_context)
        return TransformableRDD(None,
                                self.__file_type,
                                self._transformable_rdd.impute(column_index, strategy_apply),
                                sc=self.spark_context)

    def clusters(self, column_index, clustering_algorithm):
        algorithm = clustering_algorithm.get_algorithm(self.spark_context)
        return Clusters(self._transformable_rdd.clusters(column_index, algorithm))

    def list_facets_of(self, column_index):
        return TextFacets(self._transformable_rdd.listFacets(column_index))

    def list_facets(self, column_index):
        array = py2java_int_array(self.spark_context, column_index)
        return TextFacets(self._transformable_rdd.listFacets(array))


    def select(self, column_index):
        return self._transformable_rdd.select(column_index)

    def normalize(self, column_index, normalizer_strategy):
        normalizer = normalizer_strategy.get_normalizer(self.spark_context)
        return TransformableRDD(None, self.__file_type, self._transformable_rdd.normalize(column_index, normalizer),
                                sc=self.spark_context)

    def smooth(self, column_index, smoothing_method):
        method = smoothing_method.get_smoothing_method(self.spark_context)
        return self._transformable_rdd.smooth(column_index, method)

    def merge_columns(self, merge_plan):
        plan = merge_plan.get_plan(self.spark_context)
        return TransformableRDD(None, self.__file_type,
                                self._transformable_rdd.merge_columns(plan),
                                sc=self.spark_context)

    def split_column(self, split_plan):
        plan = split_plan.get_plan(self.spark_context)
        return TransformableRDD(None, self.__file_type,
                                self._transformable_rdd.splitColumn(plan),
                                sc=self.spark_context)

    def get_duplicates(self):
        return TransformableRDD(None, self.__file_type,
                                self._transformable_rdd.getDuplicates(),
                                sc=self.spark_context)

    def get_duplicates(self, column_indexes):
        return TransformableRDD(None, self.__file_type,
                                self._transformable_rdd.getDuplicates(column_indexes),
                                sc=self.spark_context)

    def drop_column(self, column_index):
        return TransformableRDD(None, self.__file_type,
                                self._transformable_rdd.dropColumn(column_index),
                                sc=self.spark_context)

