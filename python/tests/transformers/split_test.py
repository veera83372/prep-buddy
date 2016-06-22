from py_prep_buddy.transformable_rdd import TransformableRDD
from utils.python_test_case import PySparkTestCase


class SplitPlan(PySparkTestCase):
    def test_should_split_given_column_indexes(self):
        initial_dataset = self.sc.parallelize(["FirstName LastName MiddleName,850"])
        initial_rdd = TransformableRDD(initial_dataset, "csv")

        splited_column_rdd = initial_rdd.splitColumn(SplitPlan(0, " ", False))
        self.assertEquals("FirstName,LastName,MiddleName,850", splited_column_rdd.first())

