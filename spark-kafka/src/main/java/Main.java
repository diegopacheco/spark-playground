import org.apache.spark.SparkConf;

public class Main {
    public static void main(String[] args) {
		System.out.println("Running spark job... ");
		SparkConf config = new SparkConf().setAppName("word_count_job");
		config.set("bootstrapServers", args[0].replace("#", ","));
		WordCountProcessor.run(config);
	}
}