from py4j.java_gateway import java_import
from py_prep_buddy import py2java_int_array
from py_prep_buddy.class_names import ClassNames


class ModeSubstitution(object):
    def get_strategy(self, sc):
        java_import(sc._jvm, ClassNames.MODE_SUBSTITUTION)
        return sc._jvm.ModeSubstitution()


class MeanSubstitution(object):
    def get_strategy(self, sc):
        java_import(sc._jvm, ClassNames.MEAN_SUBSTITUTION)
        return sc._jvm.MeanSubstitution()


class ApproxMeanSubstitution(object):
    def get_strategy(self, sc):
        java_import(sc._jvm, ClassNames.APPROX_MEAN_SUBSTITUTION)
        return sc._jvm.ApproxMeanSubstitution()


class UnivariateLinearRegressionSubstitution(object):
    def __init__(self, column_index):
        self._column_index = column_index

    def get_strategy(self, sc):
        java_import(sc._jvm, ClassNames.UNIVARIATE_SUBSTITUTION)
        return sc._jvm.UnivariateLinearRegressionSubstitution(self._column_index)


class NaiveBayesSubstitution(object):
    def __init__(self, *column_index):
        self._column_index = column_index

    def get_strategy(self, sc):
        java_import(sc._jvm, ClassNames.NAIVE_BAYES_SUBSTITUTION)
        independent_column_indexes = py2java_int_array(sc, self._column_index)
        return sc._jvm.NaiveBayesSubstitution(independent_column_indexes)
