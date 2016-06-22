from py_prep_buddy.transformable_rdd import TransformableRDD
from py_prep_buddy.transformers.merge_plan import MergePlan
from utils.python_test_case import PySparkTestCase
import tests


class MergePlanTest(PySparkTestCase):
    def test_should_merge_given_column_indexes(self):
        initial_dataset = self.sc.parallelize(["FirstName,LastName,732,MiddleName"])
        initial_rdd = TransformableRDD(initial_dataset, "csv")

        joined_column_rdd = initial_rdd.merge_columns(MergePlan([3, 1, 0], "_", False))
        self.assertEquals("732,MiddleName_LastName_FirstName", joined_column_rdd.first())

        with_originals = initial_rdd.merge_columns(MergePlan([3, 1, 0], "_", True))
        self.assertEquals("FirstName,LastName,732,MiddleName,MiddleName_LastName_FirstName", with_originals.first())

        joinedColumnWithDefault = initial_rdd.merge_columns(MergePlan([3, 1, 0], retain_columns=False))
        self.assertEquals("732,MiddleName LastName FirstName", joinedColumnWithDefault.first())
