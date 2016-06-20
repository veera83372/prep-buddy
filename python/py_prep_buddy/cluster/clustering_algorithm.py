class SimpleFingerprintAlgorithm(object):
    def get_algorithm(self, spark_context):
        return spark_context._jvm.org.apache.prepbuddy.cluster.SimpleFingerprintAlgorithm()


class NGramFingerprintAlgorithm(object):
    def __init__(self, n_gram):
        self.__n_gram = n_gram

    def get_algorithm(self, spark_context):
        return spark_context._jvm.org.apache.prepbuddy.cluster.NGramFingerprintAlgorithm(self.__n_gram)
