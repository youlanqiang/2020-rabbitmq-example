package top.youlanqiang.rabbitmqexample;


import com.rabbitmq.client.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@SpringBootTest
public class PubSubTests {


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

    @Test
    public void publish() throws Exception{
        Connection conn = getConnection();
        Channel channel = conn.createChannel();
        // 创建Exchange 绑定一个队列
        // param1: exchange的名称
        // param2: 指定exchange的类型 FANOUT-pubsub, DIRECT-Routing, TOPIC-Topic
        channel.exchangeDeclare("pubsub-exchange", BuiltinExchangeType.FANOUT);
        channel.queueBind("pubsub-queue1", "pubsub-exchange", "");
        channel.queueBind("pubsub-queue2", "pubsub-exchange", "");
        for (int i = 0; i < 10; i++) {
            channel.basicPublish("pubsub-exchange","",null, ("Hello,World." + i).getBytes());
        }
    }


    @Test
    public void consumer1() throws Exception{
        Connection conn =  getConnection();
        Channel channel = conn.createChannel();
        channel.queueDeclare("pubsub-queue1", true, false,false,null);

        //指定当前消费者，一次可以消费多少个消息
        channel.basicQos(1);

        DefaultConsumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("消费者1号接收到的消息:" + new String(body, StandardCharsets.UTF_8));
                //手动ACK
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };

        channel.basicConsume("pubsub-queue1",false, consumer);

        System.in.read();
        //5.释放资源
        channel.close();
        conn.close();
    }

    @Test
    public void consumer2() throws Exception{
        Connection conn =  getConnection();
        Channel channel = conn.createChannel();
        channel.queueDeclare("pubsub-queue2", true, false,false,null);
        channel.basicQos(1);

        DefaultConsumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                try {
                    TimeUnit.SECONDS.sleep(1);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                System.out.println("消费者2号接收到的消息:" + new String(body, StandardCharsets.UTF_8));
                channel.basicAck(envelope.getDeliveryTag(), false);
            }
        };

        channel.basicConsume("pubsub-queue2",false, consumer);

        System.in.read();
        //5.释放资源
        channel.close();
        conn.close();
    }

}
























































