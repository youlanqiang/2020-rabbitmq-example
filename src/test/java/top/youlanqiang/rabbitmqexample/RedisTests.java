package top.youlanqiang.rabbitmqexample;


import com.rabbitmq.client.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.params.SetParams;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

@SpringBootTest
public class RedisTests {

    public static Connection getConnection(){
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setVirtualHost("/test");
        connectionFactory.setHost("192.168.81.131");
        connectionFactory.setPort(5672);
        connectionFactory.setUsername("test");
        connectionFactory.setPassword("test");
        Connection connection = null;
        try {
            connection = connectionFactory.newConnection();
        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
        return connection;
    }

    //单个Confirm
    @Test
    void publisher() throws IOException, TimeoutException, InterruptedException {
        //1. 获取连接对象
        Connection conn = getConnection();

        //2. 创建Channel
        Channel channel = conn.createChannel();

        channel.addReturnListener(new ReturnListener() {
            @Override
            public void handleReturn(int replyCode, String replyText, String exchange, String routingKey, AMQP.BasicProperties properties, byte[] body) throws IOException {
                //当消息没有送达到queue时，才会执行。
                System.out.println(new String(body, StandardCharsets.UTF_8)+", 没有送达到queue中.");
            }
        });

        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .deliveryMode(1) //指定消息是否需要持久化 1-需要持久化 2-不需要持久化
                .messageId(UUID.randomUUID().toString())
                .build();

        //开启Confirm
        channel.confirmSelect();

        //3. 发布消息到Exchange
        //发送消息时，mandatory要设置为true
        channel.basicPublish("","HelloWorld",true,properties, "Hello,World.".getBytes());

        //判断消息是否成功.
        if(channel.waitForConfirms()){
            System.out.println("消息发送成功.");
        }else{
            System.out.println("发送消息失败.");
        }

        //4. 释放资源
        channel.close();
        conn.close();
    }


    @Test
    public void consumer() throws Exception{
        Connection conn =  getConnection();
        Channel channel = conn.createChannel();
        channel.queueDeclare("HelloWorld", true, false,false,null);

        DefaultConsumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                Jedis jedis = new Jedis("192.168.81.131", 6379);

                String messageId =  properties.getMessageId();
                //1. setnx 到redis中，默认指定value-0
                String result = jedis.set(messageId,"0", SetParams.setParams().nx().ex(10));
                if(result!=null&&result.equalsIgnoreCase("OK")){
                    System.out.println("接收到的消息:" + new String(body, StandardCharsets.UTF_8));
                    //消费成功执行
                    jedis.set(messageId, "1");
                    channel.basicAck(envelope.getDeliveryTag(),false);
                }else{
                    //告诉rabbitMQ被消费.
                    String s = jedis.get(messageId);
                    if("1".equalsIgnoreCase(s)){
                        channel.basicAck(envelope.getDeliveryTag(),false);
                    }
                }

                //2. 消费成功， set messageId 1
                //3. 如果1中的setnx失败，获取key对应的value，如果是0，return，如果是1，ack
            }
        };

        channel.basicConsume("HelloWorld",true, consumer);

        System.in.read();
        //5.释放资源
        channel.close();
        conn.close();
    }


}
