from py4j.java_gateway import java_import

from py_prep_buddy.classnames import ClassNames


class SimpleMovingAverage(object):
    def __init__(self, window_size):
        self.__window_size = window_size

    def get_smoothing_method(self, spark_context):
        java_import(spark_context._jvm, ClassNames.SIMPLE_MOVING_AVERAGE)
        return spark_context._jvm.SimpleMovingAverageMethod(self.__window_size)
