package org.example;

import org.apache.kafka.clients.admin.*;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.Duration;
import java.util.*;

public class KafkaConsumerLatencyTest {

    private static final String TOPIC = "test-topic";
    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final int TARGET_MESSAGES = 1000;

    public static void main(String[] args) throws Exception {

        // ── Step 1: Delete all messages except the last 1000 ──────────────────
        trimTopicToLastN(TARGET_MESSAGES);

        // ── Step 2: Set up consumer ───────────────────────────────────────────
        Properties props = new Properties();
        props.put("bootstrap.servers", BOOTSTRAP_SERVERS);
        props.put("group.id", "latency-test-group-" + System.currentTimeMillis()); // fresh group = no committed offset
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");   // read from the beginning of what's left
        props.put("enable.auto.commit", "false");
        props.put("max.poll.records", "1");            // one record per poll → one timing per call

        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));

        // ── Step 3: Consume exactly 1000 messages and time each call ──────────
        long[] responseTimes = new long[TARGET_MESSAGES];
        int count = 0;

        System.out.println("Starting consume latency test — consuming " + TARGET_MESSAGES + " messages...");

        while (count < TARGET_MESSAGES) {
            long start = System.currentTimeMillis();
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(5000));
            long elapsed = System.currentTimeMillis() - start;

            if (records.isEmpty()) {
                System.err.println("Poll returned no records at count=" + count + ". Check that the topic has enough messages.");
                break;
            }

            // max.poll.records=1 → exactly one record per iteration
            responseTimes[count] = elapsed;
            count++;

            if (count % 100 == 0) {
                System.out.println("Consumed " + count + " messages...");
            }
        }

        consumer.close();

        if (count < TARGET_MESSAGES) {
            System.err.println("Warning: only " + count + " messages were consumed.");
        }

        // ── Step 4: Compute & report statistics ───────────────────────────────
        long[] sorted = Arrays.copyOf(responseTimes, count);
        Arrays.sort(sorted);

        long median = sorted[sorted.length / 2];

        long sum = 0;
        for (long t : sorted) sum += t;
        double mean = (double) sum / sorted.length;

        long min = sorted[0];
        long max = sorted[sorted.length - 1];
        long p95 = sorted[(int) (sorted.length * 0.95)];
        long p99 = sorted[(int) (sorted.length * 0.99)];

        String resultMedian = "Median Consume Response Time : " + median + " ms";
        String resultMean   = "Mean   Consume Response Time : " + mean   + " ms";
        String resultMin    = "Min    Consume Response Time : " + min    + " ms";
        String resultMax    = "Max    Consume Response Time : " + max    + " ms";
        String resultP95    = "P95    Consume Response Time : " + p95    + " ms";
        String resultP99    = "P99    Consume Response Time : " + p99    + " ms";

        System.out.println("\n=== Consume Latency Results ===");
        System.out.println(resultMedian);
        System.out.println(resultMean);
        System.out.println(resultMin);
        System.out.println(resultMax);
        System.out.println(resultP95);
        System.out.println(resultP99);

        // ── Step 5: Append results to the same results.txt the producer used ──
        try (PrintWriter out = new PrintWriter(new FileWriter("results.txt", true))) {
            out.println("\n=== Consume Latency Results ===");
            out.println(resultMedian);
            out.println(resultMean);
            out.println(resultMin);
            out.println(resultMax);
            out.println(resultP95);
            out.println(resultP99);
            out.println("\nDetailed Consume Response Times (ms):");
            for (long t : sorted) out.println(t);
        } catch (IOException e) {
            System.err.println("Error writing to results.txt: " + e.getMessage());
        }

        System.out.println("\nResults appended to results.txt");
    }

    /**
     * Moves the beginning offset of every partition in the topic forward so
     * that only the last {@code keepCount} messages remain readable.
     * Uses Kafka's Admin API (deleteRecords) — no data is physically removed
     * but offsets before the new low-watermark become invisible to consumers.
     */
    private static void trimTopicToLastN(int keepCount) throws Exception {
        System.out.println("Trimming topic so only the last " + keepCount + " messages remain...");

        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);

        try (AdminClient admin = AdminClient.create(adminProps)) {

            // 1. Get end offsets for every partition
            List<TopicPartition> partitions = new ArrayList<>();
            admin.describeTopics(Collections.singletonList(TOPIC))
                 .allTopicNames().get()
                 .get(TOPIC).partitions()
                 .forEach(p -> partitions.add(new TopicPartition(TOPIC, p.partition())));

            // Use a throw-away consumer just to call endOffsets()
            Properties tempProps = new Properties();
            tempProps.put("bootstrap.servers", BOOTSTRAP_SERVERS);
            tempProps.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            tempProps.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
            tempProps.put("group.id", "trim-helper-" + System.currentTimeMillis());

            Map<TopicPartition, Long> endOffsets;
            try (KafkaConsumer<String, String> tmp = new KafkaConsumer<>(tempProps)) {
                endOffsets = tmp.endOffsets(partitions);
            }

            // 2. Calculate total messages and decide how many to skip per partition
            long totalMessages = endOffsets.values().stream().mapToLong(Long::longValue).sum();
            System.out.println("Total messages currently in topic: " + totalMessages);

            long toDelete = Math.max(0, totalMessages - keepCount);
            System.out.println("Messages to make invisible (delete from head): " + toDelete);

            if (toDelete == 0) {
                System.out.println("Nothing to trim.");
                return;
            }

            // 3. Build deleteRecords map — advance the low-watermark per partition
            //    proportionally (simple: remove from partition 0 first, then 1, …)
            Map<TopicPartition, RecordsToDelete> deleteMap = new LinkedHashMap<>();
            long remaining = toDelete;

            for (TopicPartition tp : partitions) {
                if (remaining <= 0) break;
                long endOffset = endOffsets.get(tp);
                long deleteUpTo = Math.min(remaining, endOffset);
                deleteMap.put(tp, RecordsToDelete.beforeOffset(deleteUpTo));
                remaining -= deleteUpTo;
            }

            admin.deleteRecords(deleteMap).all().get();
            System.out.println("Trim complete. Topic now has ~" + keepCount + " readable messages.\n");
        }
    }
}