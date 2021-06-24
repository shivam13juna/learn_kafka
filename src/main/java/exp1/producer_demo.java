package exp1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class producer_demo {
    public static void main(String[] args) {
        Properties properties = new Properties();

        String bootstrapServers = "localhost:9092";

        // This is how we used to do things old way
        //        properties.setProperty("bootstrap.servers", bootstrapServers);
        //        properties.setProperty("key.serializer", StringSerializer.class.getName());
        //        properties.setProperty("value.serializer", StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        ProducerRecord<String, String> record = new ProducerRecord<String, String>("first_topic", "hello_world");

        producer.send(record);
//        producer.flush();
        producer.close();





    }
}
