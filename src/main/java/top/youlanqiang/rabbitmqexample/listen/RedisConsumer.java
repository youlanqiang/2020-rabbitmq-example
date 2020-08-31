package top.youlanqiang.rabbitmqexample.listen;

import com.rabbitmq.client.Channel;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * RabbitMQ的事务
 * 事务可以保证消息100%传递，可以通过事务的回滚去记录日志，后面定时再次发送当前消息。
 * 事务的操作，效率会很低，加了事务操作后，效率会慢上100倍.
 *
 * RabbitMQ提供了Confirm的确认机制，这个效率比事务高很多.
 * 1. 普通Confirm方式
 * 2. 批量Confirm方式
 * 3. 异步Confirm方式
 */

@Component
public class RedisConsumer {

    @Autowired
    StringRedisTemplate redisTemplate;

    @RabbitListener(queues = "boot-queue")
    public void getMessage(String msg, Channel channel, Message message) throws IOException {
        //获取MessageId
        String messageId = message.getMessageProperties().getHeader("spring_returned_message_correlation");
        //1.设置key到Redis
        if(redisTemplate.opsForValue().setIfAbsent(messageId, "0",10, TimeUnit.SECONDS)){
            //2.消费消息
            System.out.println("接收到的消息:"+msg);
            //3.设置Key的value为1
            redisTemplate.opsForValue().set(messageId, "1");
            //4.手动ACK
            channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);

        }else{
            if("1".equalsIgnoreCase(redisTemplate.opsForValue().get(messageId))){
                //手动ACK
                channel.basicAck(message.getMessageProperties().getDeliveryTag(), false);

            }
        }

    }



}
