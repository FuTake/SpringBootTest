package threadtest.controller;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * nothing
 *
 * @ClassName CountDownLatchTest
 * @Description
 * @Author zsks
 * @Date 2021/12/5 10:04
 * @Version 1.0
 **/
public class CountDownLatchTest {

    static final CountDownLatch latch = new CountDownLatch(20);

    public static void main(String[] args) throws InterruptedException {
        ThreadPoolExecutor executor = getThreadPool();
        for(int i=0; i<20; i++){
            final int j = i;
            executor.submit(()->{
                try {
                    Thread.sleep(1000);
                    System.out.println(Thread.currentThread().getName()+ " - " + j);
                    latch.countDown();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            });
        }
        latch.await();
        System.out.println("ok");



    }

    public static ThreadPoolExecutor getThreadPool(){
        return new ThreadPoolExecutor(1, 3, 1000, TimeUnit.MILLISECONDS
        , new LinkedBlockingQueue<>(8), new ThreadPoolExecutor.CallerRunsPolicy());
    }

}
