package org.example.jmspart;

import java.util.ArrayList;
import java.util.List;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

public class ResponseTimeTest {

  public static void main(String[] args) throws JMSException {
    Connection conn = JMSHelper.createConnection();
    Session session = JMSHelper.createSession(conn);
    Queue queue = JMSHelper.createQueue(session);
    MessageProducer producer = JMSHelper.createProducer(session, queue);

    List<Long> ProduceResponseTimes = new ArrayList<>();

    byte[] messageContent = new byte[1024]; // 1 KB message

    BytesMessage msg = session.createBytesMessage();
    msg.writeBytes(messageContent);

    for (int i = 0; i < 1000; i++) {
      long startTime = System.nanoTime();
      producer.send(msg);
      long endTime = System.nanoTime();
      ProduceResponseTimes.add((endTime - startTime) / 1_000);
    }

    producer.close();
    session.close();
    conn.close();

    JMSHelper.printResults(ProduceResponseTimes, "JMS_Producer_Response_Times");

    long medianProduceTime = JMSHelper.median(ProduceResponseTimes);
    System.out.println("Median Produce Response Time: " + medianProduceTime + "  μs");
    System.out.println("Max Produce Response Time: " + ProduceResponseTimes.getLast() + "  μs");

    JMSHelper.clearQueue();

    conn = JMSHelper.createConnection();
    session = JMSHelper.createSession(conn);
    queue = JMSHelper.createQueue(session);
    producer= JMSHelper.createProducer(session, queue);

    for (int i = 0; i < 1000; i++) {
      producer.send(msg);
    }

    producer.close();

    List<Long> ConsumeResponseTimes = new ArrayList<>();

    MessageConsumer consumer = JMSHelper.createConsumer(session, queue);
    for (int i = 0; i < 1000; i++) {
      long startTime = System.nanoTime();
      Message receivedMsg = consumer.receive();
      long endTime = System.nanoTime();
      ConsumeResponseTimes.add((endTime - startTime) /1_000);
    }

    consumer.close();
    session.close();
    conn.close();

    JMSHelper.printResults(ConsumeResponseTimes, "JMS_Consumer_Response_Times");
    long medianConsumeTime = JMSHelper.median(ConsumeResponseTimes);
    System.out.println("Median Consume Response Time: " + medianConsumeTime + "  μs");
    System.out.println("Max Consume Response Time: " + ConsumeResponseTimes.getLast() + "  μs");

  }

}
