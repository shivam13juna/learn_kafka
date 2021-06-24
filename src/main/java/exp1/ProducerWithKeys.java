package exp1;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerWithKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        Logger logger = LoggerFactory.getLogger(ProducerWithCallback.class);

        Properties properties = new Properties();

        String bootstrapServers = "localhost:9092";

        // This is how we used to do things old way
        //        properties.setProperty("bootstrap.servers", bootstrapServers);
        //        properties.setProperty("key.serializer", StringSerializer.class.getName());
        //        properties.setProperty("value.serializer", StringSerializer.class.getName());

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);


        for (int i = 0; i < 10; i++) {

            String topic = "first_topic";
            String value = "Hello World";
            String key = "id" + i;
            ProducerRecord<String, String> record = new ProducerRecord<>("first_topic", key, value);
            logger.info("Key: " + key);

            //We're trying to send data synchronously
            producer.send(record, (recordMetadata, e) -> {
                //executes record is successfully sent or an exception is thrown
                if (e == null) {
                    // record was successfully sent
                    logger.info("Received new metadata. \n" +
                            "Topic: " + recordMetadata.topic() +
                            "\nPartition: " + recordMetadata.partition() + "\n" +
                            "Offsets: " + recordMetadata.offset() +
                            "\n Timestamp: " + recordMetadata.timestamp());
                } else {
                    logger.error("Error while producing");

                }
            }).get();
        }
//        producer.flush();
        producer.close();





    }
}
