package org.apache.prepbuddy.datacleansers;

import org.apache.prepbuddy.utils.RowRecord;

public interface ImputationStrategy {
    MissingDataHandler REMOVE_ROWS = new MissingDataHandler() {
        @Override
        public String handleMissingData(RowRecord record) {
            return null;
        }
    };

    MissingDataHandler SUSBTITUTE_WITH_MEAN = new MissingDataHandler() {
        @Override
        public String handleMissingData(RowRecord record) {
            return null;
        }
    };
    MissingDataHandler SUSBTITUTE_WITH_MOST_FREQUENT_ITEM = new MissingDataHandler() {
        @Override
        public String handleMissingData(RowRecord record) {
            return null;
        }
    };
}
