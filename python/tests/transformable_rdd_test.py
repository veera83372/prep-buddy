from utils.pythontestcase import PySparkTestCase
from py_prep_buddy.transformableRDD import TransformableRDD
import tests


class UnitTestForDeDuplication(PySparkTestCase):
    def test_uniform(self):
        rdd = self.sc.textFile("../../data/calls.csv")
        count = rdd.count()
        transformable_rdd = TransformableRDD(rdd)
        deduplicate_count = transformable_rdd.deduplicate().count()
        self.assertEquals(count, deduplicate_count)
