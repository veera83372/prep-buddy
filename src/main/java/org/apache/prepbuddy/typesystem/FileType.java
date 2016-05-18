package org.apache.prepbuddy.typesystem;


import org.apache.commons.lang.StringUtils;
import org.apache.prepbuddy.datacleansers.MissingDataHandler;
import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.utils.RowRecord;
import org.apache.spark.api.java.JavaDoubleRDD;

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

        @Override
        public String appendDelimiter(String row) {
            return row + ",";
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

        @Override
        public String appendDelimiter(String row) {
            return row + "\t";
        }
    };

    public abstract String[] parseRecord(String record);

    public abstract String join(String[] record);


    public abstract String appendDelimiter(String row);


}
