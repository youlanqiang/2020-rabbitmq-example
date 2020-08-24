package top.youlanqiang.rabbitmqexample;

import com.rabbitmq.client.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;


@SpringBootTest
class HelloWorldTests {

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
    void testOne() {
        Connection connection = getConnection();
        try {
            connection.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Test
    void helloWorldPublisher() throws IOException, TimeoutException {
        //1. 获取连接对象
        Connection conn = getConnection();

        //2. 创建Channel
        Channel channel = conn.createChannel();

        //3. 发布消息到Exchange
        // param1:指定exchange,使用"",
        // param2:指定路由规则,使用具体的队列名称
        // param3:指定传递的消息所携带的properties，使用null
        // param4:指定发布的具体消息，byte[]类型
        channel.basicPublish("","HelloWorld",null, "Hello,World.".getBytes());
        //exchange是不会将消息持久化到本地的.queue才会持久化消息.

        //4. 释放资源
        channel.close();
        conn.close();
    }

    @Test
    public void helloWorldConsumer() throws Exception{
        //1.获取连接
        Connection conn =  getConnection();
        //2.创建channel
        Channel channel = conn.createChannel();
        //3.声明队列HelloWorld(创建队列)
        // param1: queue 队列名称
        // param2: durable 当前队列是否持久化
        // param3: exclusive 是否排外,close之后，队列会自动删除,当前队列只能被一个消费者消费
        // param4: autoDelete 如果这个队列没有消费者在消费，队列自动删除
        // param5: arguments 指定当前队列的其他信息
        channel.queueDeclare("HelloWorld", true, false,false,null);

        //4.开启监听Queue
        // param1: queue 指定的那个队列
        // param2: autoAck 指定是否自动ACK （true,接收到消息后，会立即告诉RabbitMQ）
        // param3: consumer 指定消费回调

        DefaultConsumer consumer = new DefaultConsumer(channel){
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.println("接收到的消息:" + new String(body, StandardCharsets.UTF_8));
            }
        };

        channel.basicConsume("HelloWorld",true, consumer);

        System.in.read();
        //5.释放资源
        channel.close();
        conn.close();
    }

}
