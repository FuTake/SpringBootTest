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

/**
 * @author zhg
 * @date 2023/8/24
 *
 *
 */

@RocketMQMessageListener(topic = "rocketmq-test", consumeThreadMax = 1,consumerGroup = "rocketmq-consumer-2",consumeMode = ConsumeMode.CONCURRENTLY, messageModel = MessageModel.CLUSTERING)
@Component
@Order(2)
public class MsgListener2Two implements RocketMQListener<MessageExt> {

    private static final Logger log = LoggerFactory.getLogger(MsgListener2Two.class);

    @Override
    public void onMessage(MessageExt messageExt) {
        String content = new String(messageExt.getBody(), StandardCharsets.UTF_8);
        log.info("msgListener2-2 content = " + messageExt.getMsgId() + " - " + messageExt.getQueueId() + " - " + content);
    }
}
