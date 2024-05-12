import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
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
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        return properties;
    }
}
