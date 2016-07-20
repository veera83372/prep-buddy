from py_prep_buddy.rdds.transformable_rdd import TransformableRDD
from py_prep_buddy.transformers.split_plan import SplitPlan
from utils.python_test_case import PySparkTestCase
import tests


class SplitPlanTest(PySparkTestCase):
    def test_should_split_given_column_indexes(self):
        initial_dataset = self.sc.parallelize(["FirstName LastName MiddleName,850"])
        initial_rdd = TransformableRDD(initial_dataset, "csv")
        plan = SplitPlan(0, " ", False)
        splited_column_rdd = initial_rdd.split_column(plan)
        self.assertEquals("FirstName,LastName,MiddleName,850", splited_column_rdd.first())

        rdd_keeping_Column = initial_rdd.split_column(SplitPlan(0, " ", True))
        self.assertEquals("FirstName LastName MiddleName,FirstName,LastName,MiddleName,850",
                          rdd_keeping_Column.first())
        #
        # splitColumnByLengthRDD = initial_rdd.splitColumn(SplitPlan(0, [9, 9], False))
        # self.assertEquals("FirstName, LastName,850", splitColumnByLengthRDD.first())
        #
        # splitColumnByLengthRDDByKeepingColumn = initial_rdd.splitColumn(SplitPlan(0, [9, 9], True))
        # self.assertEqual("FirstName LastName MiddleName,FirstName, LastName,850", splitColumnByLengthRDDByKeepingColumn.first());
