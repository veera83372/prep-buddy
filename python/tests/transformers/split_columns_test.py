from utils.python_test_case import PySparkTestCase
from pyprepbuddy.rdds.transformable_rdd import TransformableRDD


class SplitColumnsTest(PySparkTestCase):
    def test_should_split_given_column_indexes_split_by_delimiter(self):
        initial_data_set = self.sc.parallelize(["FirstName LastName MiddleName,850"])
        initial_rdd = TransformableRDD(initial_data_set, "csv")

        splitted_columns = initial_rdd.split_by_delimiter(0, " ", False)
        self.assertEquals("850,FirstName,LastName,MiddleName", splitted_columns.first())

    def test_should_split_given_column_indexes_split_by_delimiter_with_retain_column(self):
        initial_data_set = self.sc.parallelize(["FirstName LastName MiddleName,850"])
        initial_rdd = TransformableRDD(initial_data_set, "csv")

        split_with_retained_columns = initial_rdd.split_by_delimiter(0, " ", True)
        self.assertEquals("FirstName LastName MiddleName,850,FirstName,LastName,MiddleName",
                          split_with_retained_columns.first())

    def test_should_split_the_given_column_by_delimiter_into_given_number_of_split(self):
        data = [
            "John\tMale\t21\t+91-4382-313832\tCanada",
            "Smith\tMale\t30\t+01-5314-343462\tUK",
            "Larry\tMale\t23\t+00-9815-432975\tUSA",
            "Fiona\tFemale\t18\t+89-1015-709854\tUSA"
        ]
        initial_data_set = self.sc.parallelize(data)
        initial_rdd = TransformableRDD(initial_data_set, "tsv")
        new_dataset = initial_rdd.split_by_delimiter(3, "-", False, 2)

        list_of_records = new_dataset.collect()

        self.assertEqual(4, list_of_records.__len__())
        self.assertTrue(list_of_records.__contains__("John\tMale\t21\tCanada\t+91\t4382-313832"))
        self.assertTrue(list_of_records.__contains__("Smith\tMale\t30\tUK\t+01\t5314-343462"))

    def test_should_split_given_column_by_field_length(self):
        data = ["John,Male,21,+914382313832,Canada", "Smith, Male, 30,+015314343462, UK",
                "Larry, Male, 23,+009815432975, USA", "Fiona, Female,18,+891015709854,USA"]
        initial_data_set = self.sc.parallelize(data)
        initial_rdd = TransformableRDD(initial_data_set, "csv")

        result = initial_rdd.split_by_field_length(3, [3, 10], False).collect()

        self.assertTrue(len(result) == 4)
        self.assertTrue(result.__contains__("John,Male,21,Canada,+91,4382313832"))
        self.assertTrue(result.__contains__("Smith,Male,30,UK,+01,5314343462"))

    def test_should_split_given_column_by_field_length_with_retained_columns(self):
        data = ["John,Male,21,+914382313832,Canada", "Smith, Male, 30,+015314343462, UK",
                "Larry, Male, 23,+009815432975, USA", "Fiona, Female,18,+891015709854,USA"]
        initial_data_set = self.sc.parallelize(data)
        initial_rdd = TransformableRDD(initial_data_set, "csv")

        result = initial_rdd.split_by_field_length(3, [3, 10], True).collect()

        self.assertTrue(len(result) == 4)
        self.assertTrue(result.__contains__("John,Male,21,+914382313832,Canada,+91,4382313832"))
        self.assertTrue(result.__contains__("Smith,Male,30,+015314343462,UK,+01,5314343462"))
