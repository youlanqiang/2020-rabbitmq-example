package top.youlanqiang.rabbitmqexample;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.connection.CorrelationData;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.io.IOException;
import java.util.UUID;

@SpringBootTest
public class SpringTests {

    @Autowired
    RabbitTemplate rabbitTemplate;



    @Test
    void testOne() throws IOException {
        rabbitTemplate.convertAndSend("boot-topic-exchange", "slow.red.dog", "红色大狼狗!!");
        System.in.read();
    }

    @Test
    void testRedis() throws IOException {
        CorrelationData id = new CorrelationData(UUID.randomUUID().toString());
        rabbitTemplate.convertAndSend("boot-topic-exchange", "slow.red.dog", "红色大狼狗!!",id);
        System.in.read();
    }

}

























