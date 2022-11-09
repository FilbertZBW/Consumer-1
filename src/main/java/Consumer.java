import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class Consumer {
  private final static String QUEUE_NAME = "threadQ";
  static protected CountDownLatch latch = new CountDownLatch(16);
  static protected ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

  public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setUsername("username");
    factory.setPassword("970422");
    factory.setVirtualHost("/");
    factory.setHost("18.237.62.10");
    factory.setPort(5672);

    final Connection connection = factory.newConnection();
    //TODO: pool implementation https://commons.apache.org/proper/commons-pool/examples.html
    Runnable runnable = () -> {
      try {
        final Channel channel = connection.createChannel();
        channel.queueDeclare(QUEUE_NAME, true, false, false, null);
        // max one message per receiver
        channel.basicQos(1);
        System.out.println(" [*] Thread waiting for messages. To exit press CTRL+C");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
          String message = new String(delivery.getBody(), StandardCharsets.UTF_8);
          channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
          System.out.println( "Callback thread ID = " + Thread.currentThread().getId() + " Received '" + message + "'");
          //save to thread safe hashmap!
          map.put(consumerTag, message);
        };
        // process messages
        channel.basicConsume(QUEUE_NAME, false, deliverCallback, consumerTag -> { });
      } catch (IOException ex) {
        Logger.getLogger(Consumer.class.getName()).log(Level.SEVERE, null, ex);
      }
      latch.countDown();
    };
//    Multithreaded load balancing
    for (int i = 0; i < 16; i++) {
      Thread receiver = new Thread(runnable);
      receiver.start();
    }
//    latch.await();
  }
}
