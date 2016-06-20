class Clusters(object):
    def __init__(self, clusters):
        self.__clusters = clusters

    def get_all_clusters(self):
        return self.__clusters.getAllClusters()