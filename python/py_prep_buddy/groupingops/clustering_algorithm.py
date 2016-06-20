class SimpleFingerprintAlgorithm(object):
    def get_algorithm(self, spark_context):
        return spark_context._jvm.org.apache.prepbuddy.groupingops.SimpleFingerprintAlgorithm()