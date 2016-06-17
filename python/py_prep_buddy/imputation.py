class ImputationStrategy(object):
    def prepare_substitute(self, t_rdd, missing_data_column):
        raise NotImplementedError

    def handle_missing_data(self, row_record):
        raise NotImplementedError
