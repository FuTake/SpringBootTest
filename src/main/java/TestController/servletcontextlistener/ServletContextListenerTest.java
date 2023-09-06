package TestController.servletcontextlistener;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.servlet.ServletContextEvent;
import javax.servlet.ServletContextListener;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * @author zhg
 * @date 2023/8/21
 */
@Component
public class ServletContextListenerTest implements ServletContextListener {

    @Value("${servletContext.param1}")
    private String value;

    private static final Logger log = LoggerFactory.getLogger(ServletContextListenerTest.class);

    @Override
    public void contextInitialized(ServletContextEvent sce) {
        LocalDateTime time = LocalDateTime.now();
        log.info(time.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")) + "ServletContextListener " + value);
        ServletContextListener.super.contextInitialized(sce);
    }
}
