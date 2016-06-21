from py_prep_buddy.package import Package


class ClassNames(object):
        STRING_TO_BYTES = Package.CONNECTOR + ".StringToBytes"
        CLUSTER = Package.CLUSTER + ".Cluster"
        CLUSTERS = Package.CLUSTER + ".Clusters"
        SIMPLE_FINGERPRINT = Package.CLUSTER + ".SimpleFingerprintAlgorithm"
        N_GRAM_FINGERPRINT = Package.CLUSTER + ".NGramFingerprintAlgorithm"
        FACET = Package.CLUSTER + ".TextFacets"
        DECIMAL_SCALING_NORMALIZER = Package.NORMALIZERS + ".DecimalScalingNormalization"
        MIN_MAX_NORMALIZER = Package.NORMALIZERS + ".MinMaxNormalizer"
        Z_SCORE_NORMALIZER = Package.NORMALIZERS + ".ZScoreNormalization"
        FileType = Package.QUALITY_ANALYSERS + ".FileType"
        TSV = Package.QUALITY_ANALYSERS + ".FileType.TSV"
        TRANSFORMABLE_RDD = Package.RDDS + ".TransformableRDD"
        BYTES_TO_STRING = Package.CONNECTOR + ".BytesToString"