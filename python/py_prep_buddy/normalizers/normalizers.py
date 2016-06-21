
class MinMaxNormalizer(object):
    def __init__(self, min_range, max_range):
        self.min_range = min_range
        self.max_range = max_range

    def get_normalizer(self, spark_context):
        return spark_context._jvm.org.apache.prepbuddy.normalizers.MinMaxNormalizer()
