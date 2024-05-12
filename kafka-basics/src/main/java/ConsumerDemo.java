import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemo {

    private static final String GROUP_ID = "my-java-app";
    private static final String TOPIC_NAME = "demo_java";

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemo.class.getSimpleName());

    public static void main(String[] args) {

        KafkaConsumer< String, String> consumer = new KafkaConsumer<>(buildProperties());

        consumer.subscribe(List.of(TOPIC_NAME));

        while(true){
            log.info("Polling messages from "+TOPIC_NAME+" topic.");

            //Kafka will wait 1000 seconds to receive data from kafka (to not overload kafka)
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

            for(ConsumerRecord<String, String> record: records){
                log.info("Key:"+ record.key()+", Value:"+record.value());
                log.info("Partition:"+ record.partition()+", Offset:"+record.offset());
            }

        }

    }


    private static Properties buildProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id", GROUP_ID);
        properties.setProperty("auto.offset.reset", "earliest");
        return properties;
    }
}
