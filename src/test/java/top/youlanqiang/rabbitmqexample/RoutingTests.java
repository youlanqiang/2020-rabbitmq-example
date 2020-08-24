package top.youlanqiang.rabbitmqexample;


import com.rabbitmq.client.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@SpringBootTest
public class RoutingTests {


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
        channel.exchangeDeclare("routing-exchange", BuiltinExchangeType.DIRECT);
        channel.queueBind("routing-queue-error", "routing-exchange", "ERROR");
        channel.queueBind("routing-queue-info", "routing-exchange", "INFO");
        for (int i = 0; i < 20; i++) {
            channel.basicPublish("routing-exchange", ThreadLocalRandom.current().nextInt() %2==0?"INFO":"ERROR",null, ("Hello,World." + i).getBytes());
        }
    }


    @Test
    public void errorConsumer1() throws Exception{
        Connection conn =  getConnection();
        Channel channel = conn.createChannel();
        channel.queueDeclare("routing-queue-error", true, false,false,null);

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

        channel.basicConsume("routing-queue-error",false, consumer);

        System.in.read();
        //5.释放资源
        channel.close();
        conn.close();
    }

    @Test
    public void consumer2() throws Exception{
        Connection conn =  getConnection();
        Channel channel = conn.createChannel();
        channel.queueDeclare("routing-queue-info", true, false,false,null);
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

        channel.basicConsume("routing-queue-info",false, consumer);

        System.in.read();
        //5.释放资源
        channel.close();
        conn.close();
    }

}
























































