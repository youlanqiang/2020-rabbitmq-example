package top.youlanqiang.rabbitmqexample;


import com.rabbitmq.client.*;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.TimeoutException;

@SpringBootTest
public class ReturnTests {

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
    void returnPublisher() throws IOException, TimeoutException, InterruptedException {
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

        //开启Confirm
        channel.confirmSelect();

        //3. 发布消息到Exchange
        //发送消息时，mandatory要设置为true
        channel.basicPublish("","HelloWorld",true,null, "Hello,World.".getBytes());

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


}
