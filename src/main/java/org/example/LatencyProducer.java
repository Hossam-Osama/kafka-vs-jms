package org.example;
import org.apache.kafka.clients.producer.*;
import java.util.Properties;

public class LatencyProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
            "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 10000; i++) {
            // Embed timestamp as the message value
            long timestamp = System.currentTimeMillis();
            ProducerRecord<String, String> record =
                new ProducerRecord<>("test-topic", String.valueOf(timestamp));
            producer.send(record);
        }

        producer.flush();
        producer.close();
        System.out.println("Done producing 10000 messages.");
    }
}