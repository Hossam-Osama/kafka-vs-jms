package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import java.time.Duration;
import java.util.*;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.IOException;

public class KafkaConsumerLatencyTest {
    public static void main(String[] args) {
        Properties consumerProps = new Properties();
        consumerProps.put("bootstrap.servers", "localhost:9092");
        consumerProps.put("group.id", "latency-test-group");
        consumerProps.put("key.deserializer", StringDeserializer.class.getName());
        consumerProps.put("value.deserializer", StringDeserializer.class.getName());
        consumerProps.put("auto.offset.reset", "earliest");
        consumerProps.put("enable.auto.commit", "false");
        consumerProps.put("max.poll.records", "2000");


        Consumer<String, String> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList("test-topic"));

        Properties producerProps = new Properties();
        producerProps.put("bootstrap.servers", "localhost:9092");
        producerProps.put("key.serializer", StringSerializer.class.getName());
        producerProps.put("value.serializer", StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<>(producerProps);

        long[] responseTimes = new long[1000]; 
        int messageCount = 0;

        try {
            while (messageCount < 1000) {

                long start = System.currentTimeMillis();
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(2000));
                long end = System.currentTimeMillis();

                int recordsConsumed = records.count(); 

                for (ConsumerRecord<String, String> record : records) {
                    if (messageCount >= 1000) {
                        break;
                    }
                    responseTimes[messageCount] = end - start;
                    producer.send(new ProducerRecord<>("test-topic", record.key(), record.value()));
                    try (PrintWriter out = new PrintWriter(new FileWriter("results.txt", true))) {
                        out.println("Response Time: " + (end - start) + " ms, Messages Consumed: " + recordsConsumed);
                    } catch (IOException e) {
                        System.err.println("Error writing to results2.txt: " + e.getMessage());
                    }
                    messageCount++;
                }
            }
        } catch (Exception e) {
            System.err.println("Error while consuming messages: " + e.getMessage());
        } finally {
            consumer.close();
            producer.close();
        }

        Arrays.sort(responseTimes);

        long median = responseTimes[responseTimes.length / 2];

        long sum = 0;
        for (long time : responseTimes) {
            sum += time;
        }
        double mean = sum / 1000.0;

        String result = "Median Consume Response Time: " + median + " ms\n" + "Mean Consume Response Time: " + mean + " ms";
        System.out.println(result);

        try (PrintWriter out = new PrintWriter(new FileWriter("results.txt", true))) {
            out.println(result);
            out.println("\nDetailed Response Times (ms):");
            for (long time : responseTimes) {
                out.println(time);
            }
        } catch (IOException e) {
            System.err.println("Error writing to results.txt: " + e.getMessage());
        }
    }
}




