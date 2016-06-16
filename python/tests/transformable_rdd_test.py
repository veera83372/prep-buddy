from utils.pythontestcase import PySparkTestCase
from py_prep_buddy.transformableRDD import TransformableRDD
import tests


class UnitTestForDeDuplication(PySparkTestCase):
    def test_uniform(self):
        rdd = self.sc.textFile("../../data/calls.csv")
        count = rdd.count()
        transformable_rdd = TransformableRDD(rdd, 'csv')
        deduplicate = transformable_rdd.deduplicate()

        self.assertEquals(count, deduplicate.count())
