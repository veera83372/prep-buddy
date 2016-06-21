from py4j.java_gateway import java_import

from py_prep_buddy.classnames import ClassNames


class MinMaxNormalizer(object):
    def __init__(self, min_range, max_range):
        self.min_range = min_range
        self.max_range = max_range

    def get_normalizer(self, spark_context):
        java_import(spark_context._jvm, ClassNames.MIN_MAX_NORMALIZER)
        return spark_context._jvm.MinMaxNormalizer()



