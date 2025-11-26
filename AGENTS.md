# Repository Guidelines

## 项目结构与模块组织
- 本模块为 Maven 子模块，artifactId 为 threadtest，继承父项目 demo。
- 业务代码位于 `src/main/java/threadtest`，Spring Boot 入口类 `App.java`。
- 资源目录当前为空，新增配置建议放入 `src/main/resources`，按环境拆分 `application-<env>.yml`。
- 测试默认放在 `src/test/java`，包路径与被测类保持一致。

## 构建、测试与本地运行
- `mvn clean package`：清理并打包，产出 `target/threadtest-*.jar`。
- `mvn spring-boot:run`：以开发模式启动，便于调试。
- `mvn test`：执行全部单测；若临时跳过可加 `-DskipTests`，提交前请补跑。
- IDE 可直接导入 Maven 项目并运行 `App.main`，确保使用 JDK 8+。

## 代码风格与命名约定
- 缩进使用 4 个空格，UTF-8 编码，注释请用中文简述业务含义。
- 包名全小写，类名大驼峰，方法与变量小驼峰，常量全大写加下划线。
- 日志优先使用 Slf4j，避免 `System.out`；异常信息要包含上下文关键字段。
- 配置文件键名使用 kebab-case，如 `thread-pool.size=10`。
- 数据库地址jdbc:mysql://127.0.0.1:33060/local_test，用户名root，密码123456
- 动态线程池配置信息在数据库中的dynamic_thread_config表中存储

## 测试编写指引
- 推荐使用 JUnit5，测试类命名 `XxxTest`，方法命名清晰说明场景。
- 并发/线程复现用例注意控制线程数量与等待时间，避免阻塞 CI。
- 引入新依赖或关键逻辑变更时补充至少 1 条验证用例。

## 提交与合并规范
- 参考现有历史提交，倾向简短中文摘要，可用示例：`feat: 新增多线程示例`、`fix: 修复启动异常`。
- PR 描述应包含变更点、测试结果、影响面；如有 issue，请关联并附复现步骤或截图。
- 保持提交粒度小且可回退，不要提交本地敏感配置或临时文件。

## 配置与安全提示
- 不要提交真实凭据，敏感信息放入本地环境变量或 `.env.local` 并在 `.gitignore` 中维护。
- JVM 参数建议通过环境变量 `JAVA_OPTS` 或 Maven Profile 管理，避免硬编码。
