from py_prep_buddy.cluster.clustering_algorithm import SimpleFingerprintAlgorithm, NGramFingerprintAlgorithm
from utils.pythontestcase import PySparkTestCase
from py_prep_buddy.transformableRDD import TransformableRDD
from py_prep_buddy.imputation import *
import tests


class UnitTestsForTransformableRDD(PySparkTestCase):
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

    def test_transformableRDD_can_impute_the_missing_values_by_ModeSubstitution(self):
        rdd = self.sc.parallelize(["Ram,23", "Joe,45", "Jill,45", "Soa,"])
        transformable_rdd = TransformableRDD(rdd, 'csv')
        imputed_rdd = transformable_rdd.impute(1, ModeSubstitution())
        self.assertTrue(imputed_rdd.collect().__contains__("Soa,45"))

    def test_transformableRDD_can_impute_the_missing_values_by_MeanSubstitution(self):
        rdd = self.sc.parallelize(["Ram,10", "Joe,45", "Jill,45", "Soa,"])
        transformable_rdd = TransformableRDD(rdd, 'csv')
        imputed_rdd = transformable_rdd.impute(1, MeanSubstitution())
        self.assertTrue(imputed_rdd.collect().__contains__("Soa,25.0"))

    def test_transformableRDD_can_impute_the_missing_values_by_ApproxMeanSubstitution(self):
        rdd = self.sc.parallelize(["Ram,10", "Joe,45", "Jill,45", "Soa,"])
        transformable_rdd = TransformableRDD(rdd, 'csv')
        imputed_rdd = transformable_rdd.impute(1, ApproxMeanSubstitution())
        self.assertTrue(imputed_rdd.collect().__contains__("Soa,25.0"))

    def test_transformableRDD_can_impute_the_missing_values_by_UnivariateLinearRegressionSubstitution(self):
        rdd = self.sc.parallelize(["60,3.1", "61,3.6", "62,3.8", "63,4", "65,4.1", "64,"])
        transformable_rdd = TransformableRDD(rdd, 'csv')
        imputed_rdd = transformable_rdd.impute(1, UnivariateLinearRegressionSubstitution(0))
        self.assertTrue(imputed_rdd.collect().__contains__("64,4.06"))

    def test_transformableRDD_can_impute_the_missing_values_by_NaiveBayesSubstitution(self):
        rdd = self.sc.parallelize(["Drew,No,Blue,Short,Male",
                                   "Claudia,Yes,Brown,Long,Female",
                                   "Drew,No,Blue,Long,Female",
                                   "Drew,No,Blue,Long,Female",
                                   "Alberto,Yes,Brown,Short,Male",
                                   "Karin,No,Blue,Long,Female",
                                   "Nina,Yes,Brown,Short,Female",
                                   "Sergio,Yes,Blue,Long,Male",
                                   "Drew,Yes,Blue,Long,"])
        transformable_rdd = TransformableRDD(rdd, 'csv')
        imputed_rdd = transformable_rdd.impute(4, NaiveBayesSubstitution(0, 1, 2, 3))
        self.assertTrue(imputed_rdd.collect().__contains__("Drew,Yes,Blue,Long,Female"))

    def test_clusters_should_give_clusters_of_given_column_index(self):
        rdd = self.sc.parallelize(["CLUSTER Of Finger print", "finger print of cluster", "finger print for cluster"])
        transformable_rdd = TransformableRDD(rdd, 'csv')
        clusters = transformable_rdd.clusters(0, SimpleFingerprintAlgorithm())
        list_of_clusters = clusters.get_all_clusters()
        one_cluster = list_of_clusters[0]
        self.assertTrue(one_cluster.__contains__("CLUSTER Of Finger print"))
        self.assertFalse(one_cluster.__contains__("finger print for cluster"))

    def test_clusters_should_give_clusters_By_n_gram_fingerprint(self):
        rdd = self.sc.parallelize(["CLUSTER Of Finger print", "finger print of cluster", "finger print for cluster"])
        transformable_rdd = TransformableRDD(rdd, 'csv')
        clusters = transformable_rdd.clusters(0, NGramFingerprintAlgorithm(1))
        list_of_clusters = clusters.get_all_clusters()
        one_cluster = list_of_clusters[0]
        self.assertTrue(one_cluster.__contains__("CLUSTER Of Finger print"))
        self.assertTrue(one_cluster.__contains__("finger print for cluster"))

    def test_list_facets_should_give_facets(self):
        initialDataset = self.sc.parallelize(["X,Y", "A,B", "X,Z", "A,Q", "A,E"])
        transformable_rdd = TransformableRDD(initialDataset)
        textFacets = transformable_rdd.listFacets(0)
        self.assertEquals(2, textFacets.count());

