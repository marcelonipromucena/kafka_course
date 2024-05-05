import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoKeys {


    private static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    private static final String TOPIC_NAME = "demo_java";


    public static void main(String[] args) {

        KafkaProducer<String, String> producer = new KafkaProducer<>(buildProperties());


        for (int y = 0; y < 2; y++) {
            for (int x = 0; x < 10; x++) {

                String key = "id_" + x;
                String value = "hello world " + x;

                ProducerRecord<String, String> producerRecord =
                        new ProducerRecord<>(TOPIC_NAME, key, value);

                producer.send(producerRecord, (recordMetadata, e) -> {
                    if (e == null) {
                        //success
                        log.info("Received new metadata\n" +
                                "key: " + key + "\n" +
                                "partition: " + recordMetadata.partition() + "\n"

                        );
                    }

                });

            }
        }


        //Tell producer to send all data and block until done --synchronous
        producer.flush();
    }


    private static Properties buildProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());


        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
//        properties.setProperty("batch.size", "400");
//        properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());
        return properties;
    }
}
