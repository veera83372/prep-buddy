from py_prep_buddy.normalizers.normalizers import MinMaxNormalizer
from py_prep_buddy.transformable_rdd import TransformableRDD
from utils.python_test_case import PySparkTestCase
import tests


class NormalizationTest(PySparkTestCase):
    def test_should_normalize_by_Min_Max_normalization(self):
        initial_dataset = self.sc.parallelize([
            "07434677419,07371326239,Incoming,211,Wed Sep 15 19:17:44 +0100 2010",
            "07641036117,01666472054,Outgoing,0,Mon Feb 11 07:18:23 +0000 1980",
            "07641036117,07371326239,Incoming,45,Mon Feb 11 07:45:42 +0000 1980",
            "07641036117,07371326239,Incoming,45,Mon Feb 11 07:45:42 +0000 1980",
            "07641036117,07681546436,Missed,12,Mon Feb 11 08:04:42 +0000 1980"])
        transformable_rdd = TransformableRDD(initial_dataset, 'csv')
        final_rdd = transformable_rdd.normalize(3, MinMaxNormalizer(0, 1))
        normalized_durations = final_rdd.select(3).collect()
        expected = ["1.0", "0.0", "0.2132701421800948", "0.2132701421800948", "0.05687203791469194"]
        self.assertEquals(expected, normalized_durations)

