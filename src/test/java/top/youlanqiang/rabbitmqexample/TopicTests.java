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
public class TopicTests {


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

        //*.red.* -》 占位符
        //fast.#  -》 通配符
        //*.*.rabbit
        channel.exchangeDeclare("topic-exchange", BuiltinExchangeType.TOPIC);
        channel.queueBind("topic-queue-1", "topic-exchange", "*.red.*");
        channel.queueBind("topic-queue-2", "topic-exchange", "fast.#");
        channel.queueBind("topic-queue-2", "topic-exchange", "*.*.rabbit");

        channel.basicPublish("topic-exchange", "fast.red.monkey",null, "快黑猴".getBytes());
        channel.basicPublish("topic-exchange", "slow.black.dog",null, "慢黑狗".getBytes());
        channel.basicPublish("topic-exchange", "fast.white.cat",null, "快白猫".getBytes());

    }


    @Test
    public void consumer1() throws Exception{
        Connection conn =  getConnection();
        Channel channel = conn.createChannel();
        channel.queueDeclare("topic-queue-1", true, false,false,null);

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

        channel.basicConsume("topic-queue-1",false, consumer);

        System.in.read();
        //5.释放资源
        channel.close();
        conn.close();
    }

    @Test
    public void consumer2() throws Exception{
        Connection conn =  getConnection();
        Channel channel = conn.createChannel();
        channel.queueDeclare("topic-queue-2", true, false,false,null);
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

        channel.basicConsume("topic-queue-2",false, consumer);

        System.in.read();
        //5.释放资源
        channel.close();
        conn.close();
    }

}
























































