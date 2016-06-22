from py4j.java_gateway import java_import

from py_prep_buddy.classnames import ClassNames


class SimpleMovingAverage(object):
    def __init__(self, window_size):
        self.__window_size = window_size

    def get_smoothing_method(self, spark_context):
        java_import(spark_context._jvm, ClassNames.SIMPLE_MOVING_AVERAGE)
        return spark_context._jvm.SimpleMovingAverageMethod(self.__window_size)


class WeightedMovingAverage(object):
    def __init__(self, window_size, weights):
        self.__window_size = window_size
        self.__python_weights = weights

    def get_smoothing_method(self, spark_context):
        java_import(spark_context._jvm, ClassNames.WEIGHTS)
        java_import(spark_context._jvm, ClassNames.WEIGHTED_MOVING_AVERAGE)
        java_weights = spark_context._jvm.Weights(self.__python_weights.limit())
        for index in range(self.__window_size):
            java_weights.add(self.__python_weights.get(index))

        return spark_context._jvm.WeightedMovingAverageMethod(self.__window_size, java_weights)


class Weights(object):
    def __init__(self, limit):
        self.__weights = []
        self.__limit = limit

    def add(self, weight):
        self.__weights.append(weight)

    def get(self, index):
        return self.__weights[index]

    def limit(self):
        return self.__limit
