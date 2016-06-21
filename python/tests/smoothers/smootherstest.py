from py_prep_buddy.smoothers.smoothing_algorithms import SimpleMovingAverage
from py_prep_buddy.transformable_rdd import TransformableRDD
from utils.python_test_case import PySparkTestCase
import tests

class SmoothersTest(PySparkTestCase):
    def test_should_smooth_data_by_Simple_Moving_Average(self):
        initial_dataset = self.sc.parallelize(
                ["52,3,53", "23,4,64", "23,5,64", "23,6,64", "23,7,64", "23,8,64", "23,9,64"], 3)
        transformable_rdd = TransformableRDD(initial_dataset, "csv")
        transformed = transformable_rdd.smooth(1, SimpleMovingAverage(3))

        excepted = 4.0
        self.assertEquals(excepted, transformed.first())

        # List<Double> expectedList = Arrays.asList(4.0, 5.0, 6.0, 7.0, 8.0);
        # assertEquals(expectedList, transformed.collect());