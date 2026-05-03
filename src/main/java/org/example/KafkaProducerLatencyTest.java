package org.example;

import org.apache.kafka.clients.producer.*;
import java.util.Properties;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.IOException;
import java.util.Arrays;
import java.io.FileWriter;
import java.io.PrintWriter;

public class KafkaProducerLatencyTest {
   public static void main(String[] args) {
       String message;
       try {
           message = new String(Files.readAllBytes(Paths.get("data/message.txt")));
       } catch (IOException e) {
           System.err.println("Error reading message.txt file: " + e.getMessage());
           return;
       }

       Properties props = new Properties();
       props.put("bootstrap.servers", "localhost:9092");
       props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
       props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

       Producer<String, String> producer = new KafkaProducer<>(props);

       long[] responseTimes = new long[1000];
       for (int i = 0; i < 1000; i++) {
           long start = System.currentTimeMillis();
           try {
               producer.send(new ProducerRecord<>("test-topic", message)).get();
           } catch (Exception e) {
               System.err.println("Error sending message: " + e.getMessage());
               continue;
           }
           responseTimes[i] = System.currentTimeMillis() - start;
       }
       producer.close();


       Arrays.sort(responseTimes);
       String resultMedian = "Median Produce Response Time: " + responseTimes[500] + " ms";
       System.out.println(resultMedian);

       long sum = 0;
       for (long time : responseTimes) {
            sum += time;
       }
       double mean = (double)sum / responseTimes.length;
       String resultMean="Mean Produce Response Time: " + mean + " ms";
       System.out.println(resultMean);


       try (PrintWriter out = new PrintWriter(new FileWriter("results.txt"))) {
           out.println(resultMedian);
           out.println(resultMean);
           out.println("\nDetailed Response Times (ms):");
           for (int i = 0; i < responseTimes.length; i++) {
               out.println(responseTimes[i]);
           }
       } catch (IOException e) {
           System.err.println("Error writing to results.txt: " + e.getMessage());
       }
   }
}
