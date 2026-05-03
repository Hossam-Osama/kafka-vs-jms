package org.example;
import org.apache.kafka.clients.consumer.*;
import java.time.Duration;
import java.util.*;

public class LatencyConsumer {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "latency-test-group");
        props.put("key.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer",
            "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList("test-topic"));

        List<Long> latencies = new ArrayList<>();

        System.out.println("Waiting for messages...");

        while (latencies.size() < 10000) {
            ConsumerRecords<String, String> records =
                consumer.poll(Duration.ofMillis(1000));

            for (ConsumerRecord<String, String> record : records) {
                long sentTime = Long.parseLong(record.value());
                long latency = System.currentTimeMillis() - sentTime;
                latencies.add(latency);

                if (latencies.size() >= 10000) break;
            }
        }

        consumer.close();

        // Calculate median
        Collections.sort(latencies);
        long median = latencies.get(latencies.size() / 2);
        System.out.println("Total messages received: " + latencies.size());
        System.out.println("Median Latency: " + median + " ms");
        System.out.println("Min Latency: " + latencies.get(0) + " ms");
        System.out.println("Max Latency: " + latencies.get(latencies.size()-1) + " ms");
    }
}