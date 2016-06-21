class Cluster(object):
    def __init__(self, cluster):
        self.__cluster = cluster

    def __contains__(self, item):
        return self.__cluster.containValue(item)

    def size(self):
        return self.__cluster.size()

