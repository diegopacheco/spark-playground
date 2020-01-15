import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

public class KafkaConsumer {

    private static Map<String, Object> kafkaParams = new HashMap<>();
    static {
      kafkaParams.put("key.deserializer", StringDeserializer.class);
      kafkaParams.put("value.deserializer", StringDeserializer.class);
      kafkaParams.put("group.id", "spark_word_count_group");
      kafkaParams.put("auto.offset.reset", "earliest");
    }

    public static JavaInputDStream<ConsumerRecord<String, String>> read(
        JavaStreamingContext streamingContext, 
        SparkConf sparkConf
    ) {
        kafkaParams.put("bootstrap.servers", sparkConf.get("bootstrapServers", "localhost:9092"));
		    JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(streamingContext,
				LocationStrategies.PreferConsistent(),
				ConsumerStrategies.<String, String>Subscribe(Arrays.asList("word_count_topic"), kafkaParams));
		    return stream;
    }
}