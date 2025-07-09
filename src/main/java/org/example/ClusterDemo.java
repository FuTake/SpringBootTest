package org.example;

import com.google.common.collect.Queues;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.JedisCluster;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.*;

public class ClusterDemo {

    private static final Logger log = LoggerFactory.getLogger(ClusterDemo.class);

    private static final int TOTAL_KEYS = 1000;
    private static final BlockingQueue<String> writtenQueue = Queues.newLinkedBlockingQueue();
    private static final BlockingQueue<String> readQueue = Queues.newLinkedBlockingQueue();

    public static void main(String[] args) throws InterruptedException {
        log.info("ClusterDemo start");

        // 1. JedisCluster 连接配置
        Set<HostAndPort> nodes = new HashSet<>();
        for (int port = 7001; port <= 7006; port++) {
            nodes.add(new HostAndPort("127.0.0.1", port));
        }
        try (JedisCluster jedis = new JedisCluster(nodes)) {

            ExecutorService pool = Executors.newFixedThreadPool(3);

            // 2a. 写线程
            pool.submit(() -> {
                try {
                    for (int i = 1; i <= TOTAL_KEYS; i++) {
                        String key = "k" + i;
                        jedis.set(key, "0");
                        writtenQueue.put(key);
                        if (i % 200 == 0) log.info("Writer put {}", key);
                        TimeUnit.MILLISECONDS.sleep(2);
                    }
                    log.info("Writer finished.");
                } catch (Exception e) {
                    log.error("Writer thread error", e);
                }
            });

            // 2b. 改线程（消费写队列，把 value=0 改为1）
            pool.submit(() -> {
                try {
                    int updateCount = 0;
                    while (updateCount < TOTAL_KEYS) {
                        String key = writtenQueue.take();
                        String val = jedis.get(key);
                        if ("0".equals(val)) {
                            // 只把0改成1
                            jedis.set(key, "1");
                            readQueue.put(key);
                            updateCount++;
                            if (updateCount % 200 == 0) log.info("Updater done {}", key);
                        } else {
                            // 没命中就重新排队
                            writtenQueue.put(key);
                            TimeUnit.MILLISECONDS.sleep(1);
                        }
                    }
                    log.info("Updater finished.");
                } catch (Exception e) {
                    log.error("Updater thread error", e);
                }
            });

            // 2c. 删线程（消费读队列，删掉 value=1 的 key）
            pool.submit(() -> {
                try {
                    int delCount = 0;
                    while (delCount < TOTAL_KEYS) {
                        String key = readQueue.take();
                        String val = jedis.get(key);
                        if ("1".equals(val)) {
                            jedis.del(key);
                            delCount++;
                            if (delCount % 200 == 0) log.info("Deleter removed {}", key);
                        } else {
                            readQueue.put(key);
                            TimeUnit.MILLISECONDS.sleep(1);
                        }
                    }
                    log.info("Deleter finished.");
                } catch (Exception e) {
                    log.error("Deleter thread error", e);
                }
            });

            // 3. 收尾
            pool.shutdown();
            pool.awaitTermination(60, TimeUnit.SECONDS);
            log.info("All done. Sample key count={}", jedis.dbSize());
        }
    }
}
