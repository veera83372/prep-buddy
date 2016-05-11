package org.apache.prepbuddy;

import org.apache.prepbuddy.encryptors.HomomorphicallyEncryptedRDD;
import org.apache.prepbuddy.rdds.TransformableRDD;
import org.apache.prepbuddy.filetypes.FileType;
import org.apache.prepbuddy.utils.EncryptionKeyPair;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

public class EncryptionMain implements Serializable {

    public static void main(String[] args) {
        if(args.length == 0) {
            System.out.println("--> File Path Need To Be Specified");
            System.exit(0);
        }
        SparkConf conf = new SparkConf().setAppName("Encryption");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String filePath = args[0];
        JavaRDD<String> csvInput = sc.textFile(filePath);

        TransformableRDD transformableRDD = new TransformableRDD(csvInput, FileType.CSV);
        EncryptionKeyPair keyPair = new EncryptionKeyPair(1024);
        HomomorphicallyEncryptedRDD encryptedRDD = transformableRDD.encryptHomomorphically(keyPair, 0);


        JavaRDD<String> decrypt = encryptedRDD.decrypt(0);


        double average = encryptedRDD.average(0);
        System.out.println("Average of the first column is = " + average);
        sc.close();
    }
}
