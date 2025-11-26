package threadtest;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * 多线程问题复现分析
 *
 * @ClassName App
 * @Description
 * @Author zsks
 * @Date 2021/12/5 8:31
 * @Version 1.0
 **/
@SpringBootApplication
public class App {
    public static void main(String[] args) {
        SpringApplication.run(App.class, args);
    }
}
