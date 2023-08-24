package rocketmqtest.listener3;

import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import rocketmqtest.listener2.MsgListener2Four;

import java.nio.charset.StandardCharsets;

/**
 * @author zhg
 * @date 2023/8/24
 */
//@RocketMQMessageListener(topic = "rocketmq-test",consumerGroup = "rocketmq-consumer-2",consumeMode = ConsumeMode.CONCURRENTLY, messageModel = MessageModel.CLUSTERING)
//@Component
public class MsgListener3Three implements RocketMQListener<MessageExt> {

    private static final Logger log = LoggerFactory.getLogger(MsgListener2Four.class);

    @Override
    public void onMessage(MessageExt messageExt) {
        String content = new String(messageExt.getBody(), StandardCharsets.UTF_8);
        log.info("msgListener2-3 content = " + messageExt.getMsgId() + " - " + messageExt.getQueueId() + " - " + content);
    }
}
