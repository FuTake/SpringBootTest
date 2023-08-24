package rocketmqtest.listener2;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

/**
 * @author zhg
 * @date 2023/8/24
 * 20240124
 * 顺序消息，不同消费者组各有一个固定的消费者同时拿一条相同的消息消费，这个消费者由发送消息时设置的queueId和rocketmq算法决定
 */
//@RocketMQMessageListener(topic = "rocketmq-test", consumeThreadMax = 1,consumerGroup = "rocketmq-consumer-2",consumeMode = ConsumeMode.CONCURRENTLY, messageModel = MessageModel.CLUSTERING)
//@Component
public class MsgListener2 implements RocketMQListener<MessageExt> {

    private static final Logger log = LoggerFactory.getLogger(MsgListener2.class);
    @Override
    public void onMessage(MessageExt messageExt) {
        String content = new String(messageExt.getBody(), StandardCharsets.UTF_8);
        log.info("msgListener2-1 content = " + messageExt.getMsgId() + " - " + messageExt.getQueueId() + " - " + content);
    }
}
