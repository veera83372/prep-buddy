package org.apache.prepbuddy.filetypes;


import org.apache.commons.lang.StringUtils;

public enum FileType {
    CSV {
        @Override
        public String[] parseRecord(String record) {
            return record.split(",", -1);
        }

        @Override
        public String join(String[] record) {
            return StringUtils.join(record, ",");
        }
    },

    TSV {
        @Override
        public String[] parseRecord(String record) {
            return record.split("\t", -1);
        }

        @Override
        public String join(String[] record) {
            return StringUtils.join(record, "\t");
        }
    };

    public abstract String[] parseRecord(String record);

    public abstract String join(String[] record);
}
