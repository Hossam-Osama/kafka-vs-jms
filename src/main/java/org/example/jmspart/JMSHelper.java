package org.example.jmspart;

import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Collections;
import java.util.List;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;

public class JMSHelper {

  private static final String BROKER_URL = "tcp://localhost:61616";
  private static final String QUEUE_NAME = "test-queue";

  public static Connection createConnection() throws JMSException {
    ConnectionFactory connectionFactory = new ActiveMQConnectionFactory(BROKER_URL);
    Connection conn = connectionFactory.createConnection();
    conn.start();
    return conn;
  }


  public static Session createSession(Connection conn) throws JMSException {
    return conn.createSession(false, Session.AUTO_ACKNOWLEDGE);
  }

  public static Queue createQueue(Session session) throws JMSException {
    return session.createQueue(QUEUE_NAME);
  }

  public static MessageProducer createProducer(Session session, Queue queue) throws JMSException {
    return session.createProducer(queue);
  }

  public static MessageConsumer createConsumer(Session session, Queue queue) throws JMSException {
    return session.createConsumer(queue);
  }

  public static void clearQueue() throws JMSException {
    Connection conn = createConnection();
    Session session = createSession(conn);
    Queue queue = createQueue(session);
    MessageConsumer consumer = createConsumer(session, queue);
    while (consumer.receive(1000) != null) {}
    consumer.close();
    session.close();
    conn.close();
  }

  public static void printResults(List<Long> responseTimes, String testName) {
    try (PrintWriter out = new PrintWriter(new FileWriter(testName + ".csv", true))) {
      out.println(testName);
      out.println("Message Number,Response Time (μs)");
      for (int i = 0; i < responseTimes.size(); i++) {
        out.println((i + 1) + "," + responseTimes.get(i));
      }
    } catch (IOException e) {
      System.err.println("Error writing results to file: " + e.getMessage());
    }
  }

    public static long median(List<Long> list) {
        Collections.sort(list);
        int size = list.size();
        if (size % 2 == 0)
            return (list.get(size/2 - 1) + list.get(size/2)) / 2;
        else
            return list.get(size/2);
    }

}
