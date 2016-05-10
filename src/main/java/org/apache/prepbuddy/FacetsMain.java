package org.apache.prepbuddy;

import org.apache.prepbuddy.filetypes.FileType;
import org.apache.prepbuddy.groupingops.GroupingOps;
import org.apache.prepbuddy.groupingops.TextFacets;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class FacetsMain {
    public static void main(String[] args) {
        if(args.length == 0) {
            System.out.println("--> File Path Need To Be Specified");
            System.exit(0);
        }
        SparkConf conf = new SparkConf().setAppName("Deduplication");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String filePath = args[0];
        JavaRDD<String> csvInput = sc.textFile(filePath);

        TextFacets textFacets = GroupingOps.listTextFacets(csvInput, 4, FileType.CSV);
        long count = textFacets.count();

        System.out.println("-->>> Total " + count);

        sc.close();
    }
}
