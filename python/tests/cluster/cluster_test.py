from py_prep_buddy.transformable_rdd import TransformableRDD
from utils.python_test_case import PySparkTestCase
import tests


class ClusterTest(PySparkTestCase):
    def test_should_give_highest_of_facets(self):
        initial_dataset = self.sc.parallelize(["X,Y", "A,B", "X,Z", "A,Q", "A,E"])
        transformable_rdd = TransformableRDD(initial_dataset)
        text_facets = transformable_rdd.list_facets(0)
        highest = text_facets.highest()
        self.assertEqual("A", highest[0]._1())
