import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WikimediaChangeHandler implements EventHandler {

    private final Logger log = LoggerFactory.getLogger(WikimediaChangeHandler.class);
    KafkaProducer<String, String> kafkaProducer;
    String topic;

    public WikimediaChangeHandler(KafkaProducer<String, String> kafkaProducer, String topic){
        this.kafkaProducer = kafkaProducer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {

    }

    @Override
    public void onClosed(){
        kafkaProducer.close();
    }

    @Override
    public void onMessage(String s, MessageEvent messageEvent){
        log.info(messageEvent.getData());
        //Stream received a message coming from http
        kafkaProducer.send(new ProducerRecord<>(topic, messageEvent.getData()));
    }

    @Override
    public void onComment(String s){

    }

    @Override
    public void onError(Throwable throwable) {
        log.error("Erro ao ler streaming", throwable);
    }
}
