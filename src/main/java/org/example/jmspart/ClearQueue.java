package org.example.jmspart;

import javax.jms.JMSException;

public class ClearQueue {
  public static void main(String[] args) throws JMSException {
      JMSHelper.clearQueue();
      System.out.println("Queue cleared successfully.");
  }
}
