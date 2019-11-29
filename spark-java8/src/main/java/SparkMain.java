import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;

public class SparkMain{
    public static void main(String args[]) throws Exception{
        
        String logFile = "/home/diego/bin/spark-2.4.4-bin-hadoop2.7/README.md";
        SparkSession spark = SparkSession.builder()
                            .appName("Simple Application")
                            .config("spark.master", "local")
                            .getOrCreate();

        Dataset<String> logData = spark.read().textFile(logFile).cache();
    
        long numAs = logData.filter( (String s) -> s.contains("a")).count();
        long numBs = logData.filter( (String s) -> s.contains("b")).count();
    
        System.out.println("Lines with a: " + numAs + ", lines with b: " + numBs);
        spark.stop();
    }
}