package threadtest.threadConfig;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 线程池
 *
 * @ClassName ThreadPool
 * @Description
 * @Author zsks
 * @Date 2021/12/5 8:33
 * @Version 1.0
 **/
@Configuration
public class ThreadPool {

    @Bean
    public ThreadPoolExecutor getThreadPool(){
        return new ThreadPoolExecutor(5, 10, 1000, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(8), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("threadPool-" + thread.getId());
                return thread;
            }
        }, new ThreadPoolExecutor.CallerRunsPolicy());
    }

}
