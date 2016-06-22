from py4j.java_gateway import java_import

from py_prep_buddy.class_names import ClassNames


class SplitPlan(object):
    def __init__(self, combination_order, separator=" ", retain_columns=True):
        self.__combination_order = combination_order
        self.__retain_columns = retain_columns
        self.__separator = separator

    def get_plan(self, spark_context):
        java_import(spark_context._jvm, ClassNames.SPLIT_PLAN)
        return spark_context._jvm.SplitPlan(self.__combination_order, self.__separator, self.__retain_columns)
