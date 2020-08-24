package top.youlanqiang.rabbitmqexample.config;

import org.springframework.amqp.core.*;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class RabbitMQConfig {

    //1.创建exchange - topic

    @Bean
    public TopicExchange getTopicExchange(){
        return ExchangeBuilder.topicExchange("boot-topic-exchange").durable(true).build();
    }

    //2.创建queue
    @Bean
    public Queue getQueue(){
        return QueueBuilder.durable("boot-queue").build();
    }

    //3.绑定在一起
    @Bean
    public Binding getBinding(TopicExchange topicExchange, Queue queue){
        return BindingBuilder.bind(queue).to(topicExchange).with("*.red.*");
    }
}
