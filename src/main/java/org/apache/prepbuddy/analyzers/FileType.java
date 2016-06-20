package org.apache.prepbuddy.analyzers;


import org.apache.commons.lang.StringUtils;

/**
 * File formats that are supported by TransformableRDD
 */
public enum FileType {
    CSV {
        @Override
        public String[] parseRecord(String record) {
            String[] recordAsArray = record.split(",", -1);
            return trimAll(recordAsArray);
        }

        @Override
        public String join(String[] record) {
            return StringUtils.join(record, ",");
        }

        @Override
        public String appendDelimiter(String row) {
            return row + ",";
        }
    },

    TSV {
        @Override
        public String[] parseRecord(String record) {
            String[] recordAsArray = record.split("\t", -1);
            return trimAll(recordAsArray);
        }

        @Override
        public String join(String[] record) {
            return StringUtils.join(record, "\t");
        }

        @Override
        public String appendDelimiter(String row) {
            return row + "\t";
        }
    };

    String[] trimAll(String[] record) {
        for (int i = 0; i < record.length; i++) {
            String each = record[i];
            record[i] = each.trim();
        }
        return record;
    }

    public abstract String[] parseRecord(String record);

    public abstract String join(String[] record);

    public abstract String appendDelimiter(String row);

}
