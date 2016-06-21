from . import py2java_int_array


class ModeSubstitution(object):
    def get_strategy(self, sc):
        return sc._jvm.org.apache.prepbuddy.cleansers.imputation.ModeSubstitution()


class MeanSubstitution(object):
    def get_strategy(self, sc):
        return sc._jvm.org.apache.prepbuddy.cleansers.imputation.MeanSubstitution()


class ApproxMeanSubstitution(object):
    def get_strategy(self, sc):
        return sc._jvm.org.apache.prepbuddy.cleansers.imputation.ApproxMeanSubstitution()


class UnivariateLinearRegressionSubstitution(object):
    def __init__(self, column_index):
        self._column_index = column_index

    def get_strategy(self, sc):
        return sc._jvm.org.apache.prepbuddy.cleansers.imputation.\
            UnivariateLinearRegressionSubstitution(self._column_index)


class NaiveBayesSubstitution(object):
    def __init__(self, *column_index):
        self._column_index = column_index

    def get_strategy(self, sc):
        independent_column_indexes = py2java_int_array(sc, self._column_index)
        return sc._jvm.org.apache.prepbuddy.cleansers.imputation.\
            NaiveBayesSubstitution(independent_column_indexes)
