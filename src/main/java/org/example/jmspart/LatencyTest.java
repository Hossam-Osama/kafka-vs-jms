package org.example.jmspart;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

public class LatencyTest {

  public static void main(String[] args) throws JMSException, InterruptedException { // Added InterruptedException
      final int NUM_MESSAGES = 10_000;
      List<Long> latencies = new ArrayList<>();


      CountDownLatch latch = new CountDownLatch(NUM_MESSAGES);

      System.out.println("Setting up JMS connection and consumer...");

      Connection conn = JMSHelper.createConnection();
      Session session = JMSHelper.createSession(conn);
      Queue queue = JMSHelper.createQueue(session);

      System.out.println("Clearing queue before test...");
      JMSHelper.clearQueue();


      System.out.println("Starting latency test with " + NUM_MESSAGES + " messages...");
      MessageConsumer consumer = JMSHelper.createConsumer(session, queue);
      consumer.setMessageListener(msg -> {
        try {
          long sendTime = msg.getLongProperty("sendTime");
          long latency = (System.nanoTime() - sendTime) / 1_000;

          synchronized (latencies) {
              latencies.add(latency);
          }
        } catch (JMSException e) {
            e.printStackTrace();
        } finally {
            latch.countDown();
        }
      });

      System.out.println("Producing messages...");
      MessageProducer producer = JMSHelper.createProducer(session, queue);
        byte[] payload = new byte[1024];
        for (int i = 0; i < NUM_MESSAGES; i++) {
            BytesMessage msg = session.createBytesMessage();
            msg.writeBytes(payload);
            msg.setLongProperty("sendTime", System.nanoTime());
            producer.send(msg);
        }

        System.out.println("All messages sent. Waiting for consumer to finish...");
        latch.await();
        System.out.println("All 10,000 messages received! Closing connections...");

        producer.close();
        consumer.close();
        session.close();
        conn.close();

        JMSHelper.printResults(latencies, "JMS_EndToEnd_Latencies");
        long medianLatency = JMSHelper.median(latencies);
        System.out.println("Median end-to-end latency: " + medianLatency + " us");
    }
}