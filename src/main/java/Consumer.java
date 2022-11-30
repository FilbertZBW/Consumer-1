import com.google.gson.JsonObject;
import com.google.gson.JsonParser;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

import redis.clients.jedis.Jedis;

public class Consumer {
  private final static String QUEUE_NAME = "threadQ";
  static protected CountDownLatch latch = new CountDownLatch(16);
  static protected ConcurrentHashMap<String, String> map = new ConcurrentHashMap<>();

  public static void main(String[] args) throws IOException, TimeoutException, InterruptedException {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setUsername("username");
    factory.setPassword("970422");
    factory.setVirtualHost("/");
    factory.setHost("54.201.48.169");
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
//          map.put(consumerTag, message);
          //TODO:
          // * 1. “For skier N, how many days have they skied this season?” --> key: skier value: day
          // * 2. “For skier N, what are the vertical totals for each ski day?” (calculate vertical as liftID*10) --> key: skiday value: a set of liftIDs
          // * 3. “For skier N, show me the lifts they rode on each ski day”
          // * 4. “How many unique skiers visited resort X on day N?”
          Jedis jedis = new Jedis("127.0.0.1", 6379);
          JsonObject jsonObject = JsonParser.parseString(message).getAsJsonObject();

          //TODO: study jedis sadd
          // study composite key
          try {
            String skierID = jsonObject.get("skierID").getAsString();
            String resortID = jsonObject.get("resortID").getAsString();
            String liftID = jsonObject.get("liftID").getAsString();
            String day = jsonObject.get("dayID").getAsString();
            // line 1-3
            jedis.sadd("skierId:" + skierID, day);  //line1: q1
            jedis.sadd("dayId:" + day, liftID);  //line1+line2: q2, q3
            jedis.sadd("resortId:" + resortID, skierID); //line1+line3: q4
            //line 1-3 end
          } catch (Exception e) {
            e.printStackTrace();
          }
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
    latch.await();


    // TODO: check information after client sent out messages. Create jedis client in the sender!!
  }
}
