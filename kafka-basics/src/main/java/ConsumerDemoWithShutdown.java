import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

public class ConsumerDemoWithShutdown {

    private static final String GROUP_ID = "my-java-app";
    private static final String TOPIC_NAME = "demo_java";

    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoWithShutdown.class.getSimpleName());

    public static void main(String[] args) {

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(buildProperties());


        final Thread mainThread = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Detected a shutdown, let's exit by calling consumer.wakeup()");
            consumer.wakeup();

            //join the main thread to allow the execution of the code in the main thread
            try {
                mainThread.join();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));


        try {
            consumer.subscribe(List.of(TOPIC_NAME));

            while (true) {
                log.info("Polling messages from " + TOPIC_NAME + " topic.");

                //Kafka will wait 1000 seconds to receive data from kafka (to not overload kafka)
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key:" + record.key() + ", Value:" + record.value());
                    log.info("Partition:" + record.partition() + ", Offset:" + record.offset());
                }

            }
        } catch (WakeupException e) {
            log.info("Consumer is starting to shut down...");
        } catch (Exception e1) {
            log.error("Unexpected exception in the consumer", e1);
        } finally {
            consumer.close();//also commit offsets
            log.info("The consumer is now gracefully shutting down...");
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
