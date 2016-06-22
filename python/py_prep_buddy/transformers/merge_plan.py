from py4j.java_gateway import java_import

from py_prep_buddy.classnames import ClassNames


class MergePlan(object):
    def __init__(self, combination_order, retain_columns=True, separator=" "):
        self.__combination_order = combination_order
        self.__retain_columns = retain_columns
        self.__separator = separator

    def get_plan(self, spark_context):
        java_import(spark_context._jvm, ClassNames.MERGE_PLAN)
        return spark_context._jvm.MergePlan(self.__combination_order, self.__retain_columns, self.__separator)
