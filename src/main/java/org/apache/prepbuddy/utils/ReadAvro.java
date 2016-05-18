package org.apache.prepbuddy.utils;

import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.transformation.MarkerPredicate;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

import java.util.List;

public class ReadAvro {
    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf().setAppName("MI").setMaster("local");
        JavaSparkContext javaSparkContext = new JavaSparkContext(sparkConf);
        SQLContext sqlContext = new SQLContext(javaSparkContext);
        DataFrame df = sqlContext.read().format("com.databricks.spark.avro")
                .load("data/new.avro");
        JavaRDD<Row> rowJavaRDD = df.toJavaRDD();
        JavaRDD<String> mapped = rowJavaRDD.map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return row.mkString(",");
            }
        });
        TransformableRDD transformableRDD = new TransformableRDD(mapped);
        TransformableRDD flag = transformableRDD.flag("*", new MarkerPredicate() {
            @Override
            public boolean evaluate(RowRecord row) {
                return true;
            }
        });
        List<String> collect = flag.collect();
        for (String s : collect) {
            System.out.println(s);
        }
//        System.out.println("collect = " + collect);
        System.out.printf(flag.first());

    }
}
