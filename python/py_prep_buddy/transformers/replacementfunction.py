class ReplacementFunction(object):
    def __init__(self, gateway, func):
        self.__gateway = gateway
        self.__func = func

    def replace(self, row_record):
        return self.__func(row_record)

    class Java:
        implements = ["spark_context.org.apache.prepbuddy.transformers.ReplacementFunction"]
