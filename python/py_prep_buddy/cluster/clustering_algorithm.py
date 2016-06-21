from py4j.java_gateway import java_import

from py_prep_buddy.classnames import ClassNames
from py_prep_buddy.package import Package


class SimpleFingerprint(object):
    def get_algorithm(self, spark_context):
        java_import(spark_context._jvm, ClassNames.SIMPLE_FINGERPRINT)
        return spark_context._jvm.SimpleFingerprintAlgorithm()


class NGramFingerprintAlgorithm(object):
    def __init__(self, n_gram):
        self.__n_gram = n_gram

    def get_algorithm(self, spark_context):
        java_import(spark_context._jvm, ClassNames.N_GRAM_FINGERPRINT)
        return spark_context._jvm.NGramFingerprintAlgorithm(self.__n_gram)

