import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SimpleApp {
    public static void main(String[] args) {
        String logFile = "data/calls.csv";
        SparkConf conf = new SparkConf().setAppName("Simple Application");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> allLines = sc.textFile(logFile);
        JavaRDD<String> distinctContent = allLines.distinct();
        distinctContent.saveAsTextFile("data/distinct_calls");
        sc.close();

        
    }
}