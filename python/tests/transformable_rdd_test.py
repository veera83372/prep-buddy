from utils.pythontestcase import PySparkTestCase
from py_prep_buddy.transformableRDD import TransformableRDD
import tests


class UnitTestForDeDuplication(PySparkTestCase):
    def test_transformableRDD_gives_a_count_of_element(self):
        rdd = self.sc.parallelize(["2", "3", "4", "5", "6", "7", "7", "7"])
        transformable_rdd = TransformableRDD(rdd, 'csv')
        self.assertEquals(8, transformable_rdd.count())

    def test_transformableRDD_can_collect_all_the_elements(self):
        rdd = self.sc.parallelize(["2", "3", "4", "5", "6", "7", "7", "7"])
        transformable_rdd = TransformableRDD(rdd, 'csv')
        self.assertEquals(["2", "3", "4", "5", "6", "7", "7", "7"], transformable_rdd.collect())

    def test_transformableRDD_can_deduplicate_the_given_list(self):
        rdd = self.sc.parallelize(["2", "3", "4", "5", "6", "7", "7", "7"])
        transformable_rdd = TransformableRDD(rdd, 'csv')
        deduplicate_rdd = transformable_rdd.deduplicate()
        self.assertEquals(6, deduplicate_rdd.count())
