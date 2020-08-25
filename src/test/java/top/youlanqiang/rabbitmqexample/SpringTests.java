package top.youlanqiang.rabbitmqexample;

import org.junit.jupiter.api.Test;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.io.IOException;

@SpringBootTest
public class SpringTests {

    @Autowired
    RabbitTemplate rabbitTemplate;

    @Test
    void testOne() throws IOException {
        rabbitTemplate.convertAndSend("boot-topic-exchange", "slow.red.dog", "红色大狼狗!!");
        System.in.read();
    }



}

























