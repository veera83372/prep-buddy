package org.apache.prepbuddy.datacleansers.dedupe;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import scala.Serializable;
import scala.Tuple2;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class DuplicateDetector implements Serializable {

    public JavaRDD apply(JavaRDD inputRecords) {
        JavaPairRDD fingerprintedRDD = inputRecords.mapToPair(new PairFunction<String, Long, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<Long, Tuple2<String, Integer>> call(String record) throws Exception {
                long fingerprint = generateFingerprint(record.toLowerCase());
                Tuple2<String, Integer> recordOnePair = new Tuple2<>(record, 1);

                return new Tuple2<>(fingerprint, recordOnePair);
            }
        });

        JavaPairRDD recordCountPairRDD = fingerprintedRDD.reduceByKey(new Function2<Tuple2<String, Integer>, Tuple2<String, Integer>, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, Integer> accumulator, Tuple2<String, Integer> current) throws Exception {
                int totalRecordOccurrence = accumulator._2() + current._2();
                return new Tuple2<>(accumulator._1(), totalRecordOccurrence);
            }
        });

        JavaPairRDD duplicateRecords = recordCountPairRDD.filter(new Function<Tuple2<String, Tuple2<String, Integer>>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, Tuple2<String, Integer>> recordFingerprintPair) throws Exception {
                Tuple2<String, Integer> recordOccurrencePair = recordFingerprintPair._2();
                Integer numberOfOccurrence = recordOccurrencePair._2();

                return numberOfOccurrence != 1;
            }
        });

        return duplicateRecords.values().map(new Function<Tuple2<String, Integer>, String>() {
            @Override
            public String call(Tuple2<String, Integer> recordCountPair) throws Exception {
                return recordCountPair._1();
            }
        });
    }

    private long generateFingerprint(String record) {
        MessageDigest md;
        try {
            md = MessageDigest.getInstance("MD5");
            md.update(record.getBytes(), 0, record.length());
            return new BigInteger(1, md.digest()).longValue();
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }
}
