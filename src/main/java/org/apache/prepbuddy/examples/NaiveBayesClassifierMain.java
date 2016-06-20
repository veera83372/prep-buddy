package org.apache.prepbuddy.examples;

import org.apache.prepbuddy.cleansers.imputation.NaiveBayesClassifier;
import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.transformers.RowPurger;
import org.apache.prepbuddy.utils.RowRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class NaiveBayesClassifierMain {
    public static void main(String[] args) {
        if (args.length == 0) {
            System.out.println("--> File Path Need To Be Specified");
            System.exit(0);
        }
        SparkConf conf = new SparkConf().setAppName("NaiveBayes").setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String filePath = args[0];
        JavaRDD<String> csvInput = sc.textFile(filePath, Integer.parseInt(args[1]));

        TransformableRDD inputRdd = new TransformableRDD(csvInput);

        TransformableRDD transformableRDD = inputRdd.removeRows(new RowPurger.Predicate() {
            @Override
            public Boolean evaluate(RowRecord record) {
                if (record.length() < 9)
                    return true;
                return record.valueAt(9).trim().isEmpty();
            }
        });

        NaiveBayesClassifier naiveBayesClassifier = new NaiveBayesClassifier(5, 6, 7);
        naiveBayesClassifier.train(transformableRDD);

        String[] testSet1 = (",,,,,THEFT,TELEPHONE THREAT,APARTMENT,,,,12,,,,,,,,,,,,,,2232").split(",");
        String test1Decision = naiveBayesClassifier.classify(new RowRecord(testSet1));
        System.out.println("test1Decision = " + test1Decision);
        sc.close();
    }
}
