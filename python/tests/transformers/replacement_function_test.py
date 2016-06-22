from py_prep_buddy.transformable_rdd import TransformableRDD
from py_prep_buddy.transformers.replacementfunction import ReplacementFunction
from utils.python_test_case import PySparkTestCase


class ReplacementFunctionTest(PySparkTestCase):
    def test_first(self):
        initial_dataset = self.sc.parallelize(["10", "12", "16", "13", "17", "19", "15", "20", "22", "19", "21", "19"],
                                              3)
        transformable_rdd = TransformableRDD(initial_dataset, "csv")
        replace = transformable_rdd.replace(0, lambda r: "15")
        expected = "15"
        actual = replace.first()
        self.assertEquals(expected, actual)
