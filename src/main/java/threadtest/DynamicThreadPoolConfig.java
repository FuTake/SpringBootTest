package threadtest;

import com.alibaba.druid.pool.DruidDataSource;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.util.CollectionUtils;

import javax.annotation.PreDestroy;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 动态线程池配置加载与缓存
 * - 定时从 dynamic_thread_config 表加载配置
 * - 使用 Caffeine 缓存配置与线程池实例，避免频繁建池
 * - 支持通过 getExecutor 获取指定名称的线程池
 */
@Configuration
@EnableScheduling
public class DynamicThreadPoolConfig implements InitializingBean {

    private static final Logger log = LoggerFactory.getLogger(DynamicThreadPoolConfig.class);
    private static final String DEFAULT_DB_URL = "jdbc:mysql://127.0.0.1:33060/local_test";
    private static final String DEFAULT_DB_USERNAME = "root";
    private static final String DEFAULT_DB_PASSWORD = "123456";
    private static final String QUERY_SQL = "select pool_name, core_size, max_size, queue_capacity, keep_alive_seconds, allow_core_timeout, rejection_policy, enabled from dynamic_thread_config";

    private final DruidDataSource dataSource;
    private final Cache<String, ThreadPoolSetting> settingCache;
    private final Cache<String, ThreadPoolTaskExecutor> executorCache;
    private final ConcurrentHashMap<String, ReentrantLock> poolLocks = new ConcurrentHashMap<>();

    public DynamicThreadPoolConfig() {
        this.dataSource = buildDataSource();
        this.settingCache = Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofMinutes(10))
                .maximumSize(128)
                .build();
        this.executorCache = Caffeine.newBuilder()
                .maximumSize(64)
                .build();
    }

    /**
     * 初始化德鲁伊数据源，允许通过系统属性或环境变量覆盖默认库信息
     */
    private DruidDataSource buildDataSource() {
        DruidDataSource druid = new DruidDataSource();
        druid.setUrl(resolveConfig("dynamic.db.url", DEFAULT_DB_URL));
        druid.setUsername(resolveConfig("dynamic.db.username", DEFAULT_DB_USERNAME));
        druid.setPassword(resolveConfig("dynamic.db.password", DEFAULT_DB_PASSWORD));
        druid.setDriverClassName("com.mysql.cj.jdbc.Driver");
        druid.setInitialSize(1);
        druid.setMaxActive(10);
        druid.setMinIdle(1);
        druid.setValidationQuery("SELECT 1");
        druid.setTestWhileIdle(true);
        druid.setTimeBetweenEvictionRunsMillis(60_000);
        return druid;
    }

    private String resolveConfig(String key, String defaultValue) {
        String envValue = System.getenv(key.toUpperCase().replace('.', '_'));
        if (envValue != null && !envValue.isEmpty()) {
            return envValue;
        }
        String propValue = System.getProperty(key);
        if (propValue != null && !propValue.isEmpty()) {
            return propValue;
        }
        return defaultValue;
    }

    private ReentrantLock lockFor(String poolName) {
        return poolLocks.computeIfAbsent(poolName, k -> new ReentrantLock());
    }

    /**
     * 便捷 main，运行时可手工修改数据库参数并调用 refreshSettings 验证线程池动态变更
     */
    public static void main(String[] args) throws Exception {
        DynamicThreadPoolConfig config = new DynamicThreadPoolConfig();
        config.afterPropertiesSet();

        String poolName = args.length > 0 ? args[0] : "default_pool";
        if (config.getExecutor(poolName) == null) {
            log.error("线程池 {} 未找到，演示终止，请先确认数据库存在有效配置", poolName);
            config.close();
            return;
        }
        log.info("启动后初始线程池状态");
        config.logExecutorState(poolName);

        log.info("启动异步任务提交线程，确保配置刷新过程中任务正常处理");
        Thread submitThread = config.startAsyncSubmitLoop(poolName);

        log.info("请在 30 秒内更新数据库 dynamic_thread_config 相关参数，然后程序将自动刷新");
        Thread.sleep(30_000L);

        log.info("开始主动刷新动态线程池配置");
        config.refreshSettings();
        config.logExecutorState(poolName);

        log.info("等待异步提交线程结束，观察任务在刷新期间是否正常执行");
        submitThread.join();
        Thread.sleep(5_000L);
        config.logExecutorState(poolName);

        log.info("演示完成，关闭数据源");
        config.close();
    }

    /**
     * 容器启动时先做一次加载
     */
    @Override
    public void afterPropertiesSet() {
        refreshSettings();
    }

    /**
     * 每分钟同步数据库配置
     */
    @Scheduled(fixedDelay = 60_000L)
    public void refreshSettings() {
        List<ThreadPoolSetting> settings = loadSettings();
        if (CollectionUtils.isEmpty(settings)) {
            return;
        }
        Map<String, ThreadPoolSetting> latest = new HashMap<>();
        for (ThreadPoolSetting setting : settings) {
            latest.put(setting.getPoolName(), setting);
            settingCache.put(setting.getPoolName(), setting);
            if (!setting.isEnabled()) {
                destroyExecutor(setting.getPoolName());
                continue;
            }
            ThreadPoolTaskExecutor executor = executorCache.getIfPresent(setting.getPoolName());
            if (executor == null) {
                executorCache.put(setting.getPoolName(), buildExecutor(setting));
                continue;
            }
            rebuildIfChanged(executor, setting);
        }
        // 清理被移除的线程池
        executorCache.asMap().keySet().forEach(poolName -> {
            if (!latest.containsKey(poolName)) {
                destroyExecutor(poolName);
            }
        });
    }

    /**
     * 基于原生连接加载动态配置
     */
    private List<ThreadPoolSetting> loadSettings() {
        List<ThreadPoolSetting> settings = new ArrayList<>();
        try (Connection connection = dataSource.getConnection();
             PreparedStatement ps = connection.prepareStatement(QUERY_SQL);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                ThreadPoolSetting setting = new ThreadPoolSetting();
                setting.setPoolName(rs.getString("pool_name"));
                setting.setCoreSize(rs.getInt("core_size"));
                setting.setMaxSize(rs.getInt("max_size"));
                setting.setQueueCapacity(rs.getInt("queue_capacity"));
                setting.setKeepAliveSeconds(rs.getInt("keep_alive_seconds"));
                setting.setAllowCoreTimeout(rs.getBoolean("allow_core_timeout"));
                setting.setRejectionPolicy(rs.getString("rejection_policy"));
                setting.setEnabled(rs.getBoolean("enabled"));
                settings.add(setting);
            }
        } catch (SQLException e) {
            log.warn("加载动态线程池配置失败", e);
        }
        return settings;
    }

    /**
     * 提供按名称获取线程池的方法
     */
    public Executor getExecutor(String poolName) {
        ThreadPoolTaskExecutor cached = executorCache.getIfPresent(poolName);
        if (cached != null) {
            return cached;
        }
        ThreadPoolSetting setting = settingCache.getIfPresent(poolName);
        if (setting == null || !setting.isEnabled()) {
            return null;
        }
        ThreadPoolTaskExecutor executor = buildExecutor(setting);
        executorCache.put(poolName, executor);
        return executor;
    }

    /**
     * 构建线程池实例
     */
    private ThreadPoolTaskExecutor buildExecutor(ThreadPoolSetting setting) {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(setting.getCoreSize());
        executor.setMaxPoolSize(setting.getMaxSize());
        executor.setQueueCapacity(setting.getQueueCapacity());
        executor.setKeepAliveSeconds(setting.getKeepAliveSeconds());
        executor.setAllowCoreThreadTimeOut(setting.isAllowCoreTimeout());
        executor.setThreadNamePrefix("dyn-" + setting.getPoolName() + "-");
        executor.setRejectedExecutionHandler(ThreadPoolPolicies.toHandler(setting.getRejectionPolicy()));
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        executor.initialize();
        return executor;
    }

    /**
     * 输出指定线程池的核心参数，便于观察刷新前后差异
     */
    private void logExecutorState(String poolName) {
        ThreadPoolTaskExecutor executor = executorCache.getIfPresent(poolName);
        if (executor == null) {
            log.warn("线程池 {} 未找到，请确认数据库是否存在配置且 enabled = 1", poolName);
            return;
        }
        ThreadPoolExecutor nativeExecutor = executor.getThreadPoolExecutor();
        log.info("线程池 {} 状态: core={}, max={}, queueCap={}, keepAlive={}, allowCoreTimeout={}, rejectedHandler={}, poolSize={}, queueSize={}",
                poolName,
                executor.getCorePoolSize(),
                executor.getMaxPoolSize(),
                nativeExecutor.getQueue().remainingCapacity() + nativeExecutor.getQueue().size(),
                executor.getKeepAliveSeconds(),
                nativeExecutor.allowsCoreThreadTimeOut(),
                nativeExecutor.getRejectedExecutionHandler().getClass().getSimpleName(),
                nativeExecutor.getPoolSize(),
                nativeExecutor.getQueue().size());
    }

    /**
     * 持续异步提交任务，观察刷新时的执行情况
     */
    private Thread startAsyncSubmitLoop(String poolName) {
        Thread submitThread = new Thread(() -> {
            for (int i = 0; i < 40; i++) {
                final int taskId = i;
                Executor executor = getExecutor(poolName);
                if (executor == null) {
                    log.warn("线程池 {} 不可用，放弃提交任务 {}", poolName, taskId);
                    sleepSilently(1_000L);
                    continue;
                }
                try {
                    executor.execute(() -> {
                        String threadName = Thread.currentThread().getName();
                        log.info("[{}] 任务 {} 开始执行，线程={}", poolName, taskId, threadName);
                        try {
                            Thread.sleep(2_000L);
                            log.info("[{}] 任务 {} 执行完成，线程={}", poolName, taskId, threadName);
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            log.warn("[{}] 任务 {} 被中断，线程={}", poolName, taskId, threadName);
                        } catch (Exception ex) {
                            log.error("[{}] 任务 {} 执行异常", poolName, taskId, ex);
                        }
                    });
                } catch (Exception ex) {
                    log.error("[{}] 任务 {} 提交失败，可能线程池正在切换", poolName, taskId, ex);
                }
                sleepSilently(1_000L);
            }
        });
        submitThread.setName("demo-submit-" + poolName);
        submitThread.start();
        return submitThread;
    }

    private void sleepSilently(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private void runSafely(Runnable task) {
        try {
            task.run();
        } catch (Exception ex) {
            log.error("迁移任务兜底执行异常", ex);
        }
    }

    /**
     * 若关键参数变更则销毁重建
     */
    private void rebuildIfChanged(ThreadPoolTaskExecutor executor, ThreadPoolSetting setting) {
        ReentrantLock lock = lockFor(setting.getPoolName());
        lock.lock();
        try {
            ThreadPoolTaskExecutor current = executorCache.getIfPresent(setting.getPoolName());
            if (current != executor) {
                return;
            }
            boolean needRebuild = executor.getCorePoolSize() != setting.getCoreSize()
                    || executor.getMaxPoolSize() != setting.getMaxSize()
                    || executor.getKeepAliveSeconds() != setting.getKeepAliveSeconds()
                    || executor.getThreadPoolExecutor().getQueue().remainingCapacity() + executor.getThreadPoolExecutor().getQueue().size() != setting.getQueueCapacity()
                    || executor.getThreadPoolExecutor().getRejectedExecutionHandler().getClass() != ThreadPoolPolicies.toHandler(setting.getRejectionPolicy()).getClass();
            if (needRebuild) {
                rebuildWithMigration(setting.getPoolName(), executor, setting);
            }
        } finally {
            lock.unlock();
        }
    }

    @PreDestroy
    public void close() {
        if (dataSource != null) {
            dataSource.close();
        }
    }

    /**
     * 安全重建线程池：先创建新池再迁移旧队列，避免丢任务
     */
    private void rebuildWithMigration(String poolName, ThreadPoolTaskExecutor oldExecutor, ThreadPoolSetting setting) {
        ThreadPoolTaskExecutor newExecutor = buildExecutor(setting);
        executorCache.put(poolName, newExecutor);
        log.info("线程池 {} 配置变更，开始安全重建", poolName);

        // 迁移队列中的待执行任务
        BlockingQueue<Runnable> oldQueue = oldExecutor.getThreadPoolExecutor().getQueue();
        List<Runnable> pending = new ArrayList<>();
        oldQueue.drainTo(pending);
        if (!pending.isEmpty()) {
            log.info("线程池 {} 待迁移任务数 {}", poolName, pending.size());
            for (Runnable task : pending) {
                try {
                    newExecutor.execute(task);
                } catch (RejectedExecutionException ex) {
                    log.warn("线程池 {} 迁移任务被新池拒绝，当前线程兜底执行", poolName, ex);
                    runSafely(task);
                } catch (Exception ex) {
                    log.error("线程池 {} 迁移任务失败，任务可能被拒绝", poolName, ex);
                }
            }
        }

        // 旧池停止接收新任务，等待已在运行的任务自行完成
        oldExecutor.shutdown();
        log.info("线程池 {} 重建完成，新池生效，旧池进入优雅关闭状态", poolName);
    }

    private void destroyExecutor(String poolName) {
        ReentrantLock lock = lockFor(poolName);
        lock.lock();
        try {
            ThreadPoolTaskExecutor executor = executorCache.getIfPresent(poolName);
            if (executor != null) {
                executorCache.invalidate(poolName);
                executor.shutdown();
            }
        } finally {
            lock.unlock();
        }
    }

    /**
     * 配置表映射
     */
    public static class ThreadPoolSetting {
        private String poolName;
        private int coreSize;
        private int maxSize;
        private int queueCapacity;
        private int keepAliveSeconds;
        private boolean allowCoreTimeout;
        private String rejectionPolicy;
        private boolean enabled;

        public String getPoolName() {
            return poolName;
        }

        public void setPoolName(String poolName) {
            this.poolName = poolName;
        }

        public int getCoreSize() {
            return coreSize;
        }

        public void setCoreSize(int coreSize) {
            this.coreSize = coreSize;
        }

        public int getMaxSize() {
            return maxSize;
        }

        public void setMaxSize(int maxSize) {
            this.maxSize = maxSize;
        }

        public int getQueueCapacity() {
            return queueCapacity;
        }

        public void setQueueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
        }

        public int getKeepAliveSeconds() {
            return keepAliveSeconds;
        }

        public void setKeepAliveSeconds(int keepAliveSeconds) {
            this.keepAliveSeconds = keepAliveSeconds;
        }

        public boolean isAllowCoreTimeout() {
            return allowCoreTimeout;
        }

        public void setAllowCoreTimeout(boolean allowCoreTimeout) {
            this.allowCoreTimeout = allowCoreTimeout;
        }

        public String getRejectionPolicy() {
            return rejectionPolicy;
        }

        public void setRejectionPolicy(String rejectionPolicy) {
            this.rejectionPolicy = rejectionPolicy;
        }

        public boolean isEnabled() {
            return enabled;
        }

        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
    }

    /**
     * 拒绝策略映射
     */
    public static final class ThreadPoolPolicies {
        private ThreadPoolPolicies() {
        }

        public static java.util.concurrent.RejectedExecutionHandler toHandler(String policy) {
            if ("caller_runs".equalsIgnoreCase(policy)) {
                return new java.util.concurrent.ThreadPoolExecutor.CallerRunsPolicy();
            }
            if ("discard_oldest".equalsIgnoreCase(policy)) {
                return new java.util.concurrent.ThreadPoolExecutor.DiscardOldestPolicy();
            }
            if ("discard".equalsIgnoreCase(policy)) {
                return new java.util.concurrent.ThreadPoolExecutor.DiscardPolicy();
            }
            return new java.util.concurrent.ThreadPoolExecutor.AbortPolicy();
        }
    }
}
