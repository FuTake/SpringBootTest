package rocketmqtest.listener;

import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.spring.annotation.ConsumeMode;
import org.apache.rocketmq.spring.annotation.MessageModel;
import org.apache.rocketmq.spring.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.core.RocketMQListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import javax.xml.ws.handler.MessageContext;
import java.nio.charset.StandardCharsets;

/**
 * @author zhg
 * @date 2023/8/24
 * msgListener1 2 3 再
 * consumeMode = ConsumeMode.CONCURRENTLY 模式时，MsgListener和MsgListenerTwo轮流获取一个消息
 * 三个listener都设置为Orderly模式时↓（测试发现设置成CONCURRENTLY 也能有序接收消息）
 * 当topic的读队列数量设置为2时，只有msgListener1和msgListener3能收到消息，2没有收到消息
 * 其中，3收集顺序消息和无序消息， 1收集顺序消息
 *
 * 控制台中将topic的读和写队列数量都设置为3时，且使用ConsumeMode.CONCURRENTLY msgListener2也能收到消息
 * 写队列为2，读队列为3时，msgListener2则收不到消息
 *
 * 如果读队列有3个，但listener只有两个时，从控制台 消费者-消费详情 中可以看到有一个listener监听了两个queue的内容
 *
 * 3个监听都设置广播模式 messageModel = MessageModel.BROADCASTING，普通消息所有监听都能收到
 *  有序消息所有监听都能收到，通过设定好的序列
 *  listener2，3设置为clustering  1设置为broadcasting时 只有1，3还是广播模式为什么？
 *  然后再改为普通消息，listener1,3能收到相同的消息，listener2单独收到一条消息(1,3则收不到这条消息) 为什么？？？
 *
 *  20240124
 *  1.listener 是consumeMode = ConsumeMode.CONCURRENTLY, messageModel = MessageModel.BROADCASTING
 *  2.listenerThree 是consumeMode = ConsumeMode.CONCURRENTLY, messageModel = MessageModel.CLUSTERING
 *  3.listenerTwo 是consumeMode = ConsumeMode.CONCURRENTLY, messageModel = MessageModel.CLUSTERING
 *  会出现 1和2收到一次相同的消息，然后1和3收到一次相同的消息，然后1单独收到一次消息
 *
 *  1.listener 是consumeMode = ConsumeMode.CONCURRENTLY, messageModel = MessageModel.BROADCASTING
 *  2.listenerThree 是consumeMode = ConsumeMode.CONCURRENTLY, messageModel = MessageModel.BROADCASTING
 *  3.listenerTwo 是consumeMode = ConsumeMode.CONCURRENTLY, messageModel = MessageModel.BROADCASTING
 *  会出现 1，2，3同时收到同一条消息
 */
//@RocketMQMessageListener(topic = "rocketmq-test", consumeThreadMax = 1,consumerGroup = "rocketmq-consumer-1",consumeMode = ConsumeMode.CONCURRENTLY, messageModel = MessageModel.CLUSTERING)
//@Component
public class MsgListener implements RocketMQListener<MessageExt> {

    private static final Logger log = LoggerFactory.getLogger(MsgListener.class);
    @Override
    public void onMessage(MessageExt messageExt) {
        String content = new String(messageExt.getBody(), StandardCharsets.UTF_8);
        log.info("msgListener1 content = " + messageExt.getMsgId() + " - " + messageExt.getQueueId() + " - " + content);
    }
}
