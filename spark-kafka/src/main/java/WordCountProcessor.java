import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

public class WordCountProcessor{

    public static void run(SparkConf config) {

        JavaStreamingContext streamingContext = new JavaStreamingContext(config, Durations.seconds(3));
        JavaInputDStream<ConsumerRecord<String, String>>  records  =  KafkaConsumer.read(streamingContext, config);

        JavaPairDStream<String, Integer> counts  = records
          .map( record -> Converter.convert(record.value()) )
          .flatMap( record -> Arrays.asList(record.getWords().split(" ")).iterator() )
          .mapToPair( new PairFunction<String, String, Integer>() {
            private static final long serialVersionUID = 1L;
            public Tuple2<String, Integer> call(String word) {
              return new Tuple2<String,Integer>(word, 1);
            }
          })
          .reduceByKey((a, b) -> a + b);
        
        System.out.println("Words : " + counts.groupByKey().toString());

        streamingContext.start();
		try {
			streamingContext.awaitTermination();
		} catch (InterruptedException e) {
			throw new RuntimeException("Error lanching job", e);
		}

    }
}