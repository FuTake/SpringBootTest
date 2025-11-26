package threadtest.controller;

import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import org.slf4j.Logger;
import threadtest.config.ConcurrentBinding;

import javax.annotation.PostConstruct;

/**
 * nothing
 *
 * @ClassName ThreadController
 * @Description
 * @Author zsks
 * @Date 2021/12/5 8:42
 * @Version 1.0
 **/
@RestController
@RequestMapping("/thread")
public class ThreadController {

    private Logger logger = LoggerFactory.getLogger("threadPoolTest");

    @Autowired
    private ThreadPoolExecutor threadpool;

    @PostConstruct
    public void test() throws ExecutionException, InterruptedException {
        // 模拟当时的场景，主线程绑定了它自己的binding
        ConcurrentBinding binding = ConcurrentBinding.getInstance();
        binding.setVariable("username", "mainThread");
        List<Future> futures = new ArrayList<>();
        for(int i=0; i<50; i++){
            futures.add(threadpool.submit(()->{
                ConcurrentBinding bindings = ConcurrentBinding.getInstance();
                bindings.setVariable("username", Thread.currentThread().getName());

                try{
                    Thread.sleep(1000);
                }catch(Exception e){
                    e.printStackTrace();
                }

                logger.info(String.format("%s - %s", Thread.currentThread().getName(), "OK"));
            }));
        }

        for(int i=0; i<futures.size(); i++){
            futures.get(i).get();
        }
        System.out.println("mainThread: " + binding.getVariable("username"));
    }

}
