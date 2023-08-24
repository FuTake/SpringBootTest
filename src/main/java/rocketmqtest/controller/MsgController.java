package rocketmqtest.controller;

import org.apache.rocketmq.client.exception.MQBrokerException;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.remoting.common.RemotingHelper;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.PostConstruct;
import java.io.UnsupportedEncodingException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * @author zhg
 * @date 2023/8/24
 */
public class MsgController {

    private static final Logger log = LoggerFactory.getLogger(MsgController.class);
    private static final String topicName = "rocketmq-test";
    private static final String test2 = "rocketmq-test2";
    private static final String brokerName = "broker-a";

    public static void main(String[] args) throws UnsupportedEncodingException, MQClientException, MQBrokerException, RemotingException, InterruptedException {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(10, 10, 5, TimeUnit.SECONDS, new ArrayBlockingQueue<>(1));
        executor.execute(()->{
            try {
//                orderMsg();
                orderMsgTest2ForOnce();
            }catch (Exception e){
                log.error("disorderMsg error", e);
            }
        });
        executor.execute(()->{
            try{
//                orderMsg();
            }catch (Exception e){
                log.error("orderMsg error", e);
            }
        });
        executor.execute(()->{
            try{
//                batchMsg();
            }catch (Exception e){
                log.error("batchMsg error", e);
            }
        });
        executor.execute(()->{
            try{
//                delayMsg();
            }catch (Exception e){
                log.error("delayMsg error", e);
            }
        });
        executor.shutdown();

    }

    /**
     * 事务消息
     */
    public static void transactionMsg() throws Exception{
        // 不能用已有的消费者组
        TransactionMQProducer producer = new TransactionMQProducer("rocketmq-test-transactionProducer");

        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            }
        });
        // Specify name server addresses.
        producer.setExecutorService(executorService);
        producer.setNamesrvAddr("localhost:9876");
        producer.setSendMsgTimeout(13000);
        producer.setCreateTopicKey("AUTO_CREATE_TOPIC_KEY");
        //Launch the instance.
        producer.start();
        String[] tags = new String[] {"TagA", "TagB", "TagC", "TagD", "TagE"};
        for (int i = 0; i < 10; i++) {
            try {
                Message msg =
                        new Message(topicName, tags[i % tags.length], "KEY" + i,
                                ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult = producer.sendMessageInTransaction(msg, null);
                System.out.printf("%s%n", sendResult);
                Thread.sleep(10);
            } catch (MQClientException | UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }

    }

    /**
     * 延时消息
     */
    public static void delayMsg() throws Exception{
        // 不能用已有的消费者组
        DefaultMQProducer producer = new
                DefaultMQProducer("rocketmq-test-delayProducer");
        // Specify name server addresses.
        producer.setNamesrvAddr("localhost:9876");
        producer.setSendMsgTimeout(13000);
        producer.setCreateTopicKey("AUTO_CREATE_TOPIC_KEY");
        //Launch the instance.
        producer.start();
        while(true) {
            try {
                Thread.sleep(1000);
                LocalDateTime time = LocalDateTime.now();
                String date = time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                Message msg = new Message(topicName, "tag1", (date + "-delayMsg").getBytes(RemotingHelper.DEFAULT_CHARSET));
                // 延时等级3 表示 延迟10s再发送
                msg.setDelayTimeLevel(3);
                SendResult sendResult = producer.send(msg);
                System.out.println("status-->" + sendResult.getSendStatus());
            }catch (Exception e){
                log.info("sendMsgError ", e);
            }
        }
    }

    /**
     * 批量消息发送
     * 批量消息 会同时发给同一个队列
     */
    public static void batchMsg() throws Exception{
        // 不能用已有的消费者组
        DefaultMQProducer producer = new
                DefaultMQProducer("rocketmq-test-batchProducer");
        // Specify name server addresses.
        producer.setNamesrvAddr("localhost:9876");
        producer.setSendMsgTimeout(13000);
        producer.setCreateTopicKey("AUTO_CREATE_TOPIC_KEY");
        //Launch the instance.
        producer.start();
        while(true) {
            try {
                LocalDateTime time = LocalDateTime.now();
                String date = time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                List<Message> msgs = new ArrayList<>();
                Message msg = new Message(topicName, "tag1", (date + "-batchMsg").getBytes(RemotingHelper.DEFAULT_CHARSET));
                msgs.add(msg);
                Thread.sleep(1000);
                Message msg1 = new Message(topicName, "tag1", (date + "-batchMsg").getBytes(RemotingHelper.DEFAULT_CHARSET));
                msgs.add(msg1);
                Thread.sleep(1000);
                Message msg2 = new Message(topicName, "tag1", (date + "-batchMsg").getBytes(RemotingHelper.DEFAULT_CHARSET));
                msgs.add(msg2);
                Thread.sleep(1000);
                SendResult sendResult = producer.send(msgs);
                System.out.println("status-->" + sendResult.getSendStatus());
            }catch (Exception e){
                log.info("sendMsgError ", e);
            }
        }
    }

    /**
     * 顺序消息发送
     * @throws Exception
     */
    public static void orderMsg() throws Exception {
        // 不能和disorderMsg使用同一个生产者组
        DefaultMQProducer producer = new
                DefaultMQProducer("rocketmq-test-orderproducer");
        // Specify name server addresses.
        producer.setNamesrvAddr("localhost:9876");
        producer.setSendMsgTimeout(13000);
        producer.setCreateTopicKey("AUTO_CREATE_TOPIC_KEY");
        //Launch the instance.
        producer.start();

        /*
            20240124:180315
            指定消息要发送到的topic,broker和写对列编号
            通过指定写队列实现消息的顺序发送，和消费
            指定queueId后，同一个消费者组，只有一个消费者会一直接收消息
        */
        MessageQueue messageQueue = new MessageQueue(topicName, brokerName, 2);
        while(true) {
            try {
                Thread.sleep(1000);
                LocalDateTime time = LocalDateTime.now();
                String date = time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                Message msg = new Message(topicName, "tag1", (date + "-orderlyMsg").getBytes(RemotingHelper.DEFAULT_CHARSET));
                // 指定消息发送到 指定的队列中，从而实现 消息的顺序消费
                SendResult sendResult = producer.send(msg, messageQueue);
                //SendResult sendResult = producer.send(msg, new MessageQueue(topicName, brokerName, 0));
                System.out.println("status-->" + sendResult.getSendStatus());
            }catch (Exception e){
                log.info("sendMsgError ", e);
            }
        }
    }
    public static void orderMsgTest2ForOnce() throws Exception {
        // 不能和disorderMsg使用同一个生产者组
        DefaultMQProducer producer = new
                DefaultMQProducer("rocketmq-test-orderproducer");
        // Specify name server addresses.
        producer.setNamesrvAddr("localhost:9876");
        producer.setSendMsgTimeout(13000);
        producer.setCreateTopicKey("AUTO_CREATE_TOPIC_KEY");
        //Launch the instance.
        producer.start();

        MessageQueue messageQueue = new MessageQueue();
        messageQueue.setBrokerName(brokerName);
        messageQueue.setTopic(test2);
        // 不指定queueId则一直给queueId=0发送消息
//        messageQueue.setQueueId(3);
        try {
            for (int i = 0; i < 4; i++) {
                Thread.sleep(1000);
                LocalDateTime time = LocalDateTime.now();
                String date = time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                Message msg = new Message(test2, null, (date + "-orderlyMsg").getBytes(RemotingHelper.DEFAULT_CHARSET));
                // 指定消息发送到 指定的队列中，从而实现 消息的顺序消费
                SendResult sendResult = producer.send(msg, messageQueue);
                System.out.println("status-->" + sendResult.getSendStatus());
            }
        }catch (Exception e){
            log.info("sendMsgError ", e);
        }
    }

    /**
     * 普通消息 发送
     * @throws Exception
     */
    public static void disorderMsg() throws Exception {
        DefaultMQProducer producer = new
                DefaultMQProducer("rocketmq-test-producer");
        // Specify name server addresses.
        producer.setNamesrvAddr("localhost:9876");
        producer.setSendMsgTimeout(13000);
        producer.setCreateTopicKey("AUTO_CREATE_TOPIC_KEY");
        //Launch the instance.
        producer.start();
        int count = 0;

        while(true) {
            try {
                Thread.sleep(10000);
                LocalDateTime time = LocalDateTime.now();
                String date = time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
                Message msg = new Message(topicName, "tag1", (date+"-disorderMsg").getBytes(RemotingHelper.DEFAULT_CHARSET));
                SendResult sendResult = producer.send(msg);
                System.out.println("status-->" + sendResult.getSendStatus() + " count:" + ++count + " time:" + date);
            }catch (Exception e){
                log.info("sendMsgError ", e);
            }
        }
    }
}
