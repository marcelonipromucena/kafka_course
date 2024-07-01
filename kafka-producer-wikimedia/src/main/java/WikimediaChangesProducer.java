import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class WikimediaChangesProducer {

    private static final String TOPIC_NAME = "wikimedia.recentchange";

    private static final String STREAM_URL = "https://stream.wikimedia.org/v2/stream/recentchange";

    private static final Logger log = LoggerFactory.getLogger(WikimediaChangesProducer.class.getSimpleName());

    public static void main(String[] args) throws InterruptedException {

        KafkaProducer<String, String> producer = new KafkaProducer<>(buildProperties());

        EventHandler eventHandler = new WikimediaChangeHandler(producer, TOPIC_NAME);

        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(STREAM_URL));
        EventSource eventSource = builder.build();

        eventSource.start();

        //WE PRODUCE FOR 10 MINUTES AND BLOCK THE PROGRAM UNTIL THEN
        TimeUnit.MINUTES.sleep(10);


    }


    private static Properties buildProperties() {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");


        return properties;
    }
}
