package top.youlanqiang.rabbitmqexample;


import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

@SpringBootTest
public class TransactionTests {

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
    void transactionPublisher() throws IOException, TimeoutException, InterruptedException {
        //1. 获取连接对象
        Connection conn = getConnection();

        //2. 创建Channel
        Channel channel = conn.createChannel();

        //开启Confirm
        channel.confirmSelect();

        //3. 发布消息到Exchange
        channel.basicPublish("","HelloWorld",null, "Hello,World.".getBytes());

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

    //批量Confirm
    @Test
    void transaction2Publisher() throws IOException, TimeoutException, InterruptedException {
        //1. 获取连接对象
        Connection conn = getConnection();

        //2. 创建Channel
        Channel channel = conn.createChannel();


        //开启Confirm
        channel.confirmSelect();

        //3. 发布消息到Exchange
        for (int i = 0; i < 100; i++) {
            channel.basicPublish("","HelloWorld",null, "Hello,World.".getBytes());
        }

        //判断消息是否成功.
        //当有一个消息发送失败，则判定为全部发送失败。
       channel.waitForConfirmsOrDie();

        //4. 释放资源
        channel.close();
        conn.close();
    }

    //异步Confirm
    @Test
    void transaction3Publisher() throws IOException, TimeoutException, InterruptedException {
        //1. 获取连接对象
        Connection conn = getConnection();

        //2. 创建Channel
        Channel channel = conn.createChannel();


        //开启Confirm
        channel.confirmSelect();

        //3. 发布消息到Exchange
        for (int i = 0; i < 100; i++) {
            channel.basicPublish("","HelloWorld",null, "Hello,World.".getBytes());
        }

        channel.addConfirmListener(new ConfirmListener() {
            @Override
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                System.out.println("消息发送成功,标识:"+deliveryTag+",是否是批量:"+multiple);
            }

            @Override
            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                System.out.println("消息发送失败,标识:"+deliveryTag+",是否是批量:"+multiple);
            }
        });
        //因为是异步，所以需要时间。
        System.in.read();
        //4. 释放资源
        channel.close();
        conn.close();
    }
}
