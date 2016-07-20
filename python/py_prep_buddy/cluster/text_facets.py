class TextFacets(object):
    def __init__(self, facet):
        self.__facet = facet

    def highest(self):
        return self.__facet.highest()

    def count(self):
        return self.__facet.count()

    def lowest(self):
        return self.__facet.lowest()

    def cardinal_values(self):
        return self.__facet.cardinalValues()

    def getFacetsBetween(self, lower_bound, upper_bound):
        return self.__facet.getFacetsBetween(lower_bound, upper_bound)
