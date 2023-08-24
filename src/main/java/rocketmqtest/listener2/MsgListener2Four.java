package rocketmqtest.listener2;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
// 20240531:154120
/**
 *
 * 和listener2使用相同的消费者组，但是订阅不同的topic
 * 此时 MsgListener2Four topic = "rocketmq-test2" consumerGroup = "rocketmq-consumer-2"
 *     MsgListener2Two topic = "rocketmq-test" consumerGroup = "rocketmq-consumer-2"
 * rocketmq控制台中则看到该消费者组只消费rocketmq-test2的消息，且只有MsgListener2Four这个消费者
 * rocketmq-test主题的rocketmq-consumer-2消费者组没有消费者，<del>说明此时MsgListener2Two这个消费者失效了</del> 它只订阅了 这个%RETRY%rocketmq-consumer-2主题
 * 手动将MsgListener2Four消费者的消费逻辑抛异常后，MsgListener2Two在重试队列中收到了消息
 * 此时rocketmq-test2的4个队列只有queueId=3、4有同一个消费者的信息，0、1则没有
 * @author zhg
 * @date 2024/5/31
 */
@RocketMQMessageListener(topic = "rocketmq-test2", consumeThreadMax = 1,consumerGroup = "rocketmq-consumer-2",consumeMode = ConsumeMode.CONCURRENTLY, messageModel = MessageModel.CLUSTERING)
@Component
@Order(3)
public class MsgListener2Four implements RocketMQListener<MessageExt> {

    private static final Logger log = LoggerFactory.getLogger(MsgListener2Four.class);
    @Override
    public void onMessage(MessageExt messageExt) {
        String content = new String(messageExt.getBody(), StandardCharsets.UTF_8);
        log.info("listener3-msgListener2-1 content = " + messageExt.getMsgId() + " - " + messageExt.getQueueId() + " - " + content);
        int i = 1/0;
    }
}
