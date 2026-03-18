package com.ruoyi.user.comm.config;




import com.alibaba.druid.pool.DruidDataSource;
import lombok.Data;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.annotation.PostConstruct;
import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * TDengine 配置类（纯 JDBC + Druid 连接池）
 * 子模块可通过配置文件覆盖默认参数
 */
@Data
@Configuration
@Slf4j
public class TDengineConfig {
    // ========== TDengine 基础连接参数 ==========
    @Value("${tdengine.url:jdbc:TAOS://localhost:6030/ieims?charset=utf-8}")
    private String tdUrl;

    @Value("${tdengine.username:root}")
    private String tdUsername;

    @Value("${tdengine.password:Guet@90-=}") // 默认密码（可被子模块覆盖）
    private String tdPassword;

    // ========== Druid 连接池参数（适配 Druid 命名规范） ==========
    @Value("${tdengine.druid.initial-size:2}") // 初始化连接数
    private int initialSize;

    @Value("${tdengine.druid.max-active:10}") // 最大活跃连接数
    private int maxActive;

    @Value("${tdengine.druid.min-idle:2}") // 最小空闲连接数
    private int minIdle;

    @Value("${tdengine.druid.max-wait:30000}") // 获取连接最大等待时间（ms）
    private long maxWait;

    @Value("${tdengine.druid.time-between-eviction-runs-millis:60000}") // 连接检测间隔
    private long timeBetweenEvictionRunsMillis;

    @Value("${tdengine.druid.min-evictable-idle-time-millis:300000}") // 连接最小空闲时间
    private long minEvictableIdleTimeMillis;

    @Value("${tdengine.druid.test-while-idle:true}") // 空闲时检测连接有效性
    private boolean testWhileIdle;

    @Value("${tdengine.druid.test-on-borrow:false}") // 获取连接时不检测（提升性能）
    private boolean testOnBorrow;

    @Value("${tdengine.druid.test-on-return:false}") // 归还连接时不检测
    private boolean testOnReturn;

    // 2. 新增驱动类名配置项，使用 3.x 版本的驱动类名
    @Value("${tdengine.driver-class-name:com.taosdata.jdbc.rs.RestfulDriver}")
    private String tdDriverClassName;
    // 初始化时验证驱动类
    @PostConstruct
    public void checkTDengineDriver() {

        try {
            // JDK 17 下强制使用系统类加载器加载驱动
            ClassLoader systemClassLoader = ClassLoader.getSystemClassLoader();
            Class<?> driverClass = Class.forName(tdDriverClassName, true, systemClassLoader);
            log.info("✅ TDengine 驱动类加载成功！类名：{}，类加载器：{}", tdDriverClassName, driverClass.getClassLoader());
        } catch (ClassNotFoundException e) {
            log.error("❌ TDengine 驱动类加载失败！请检查：1. Maven 依赖是否下载成功 2. 驱动类名是否匹配 TDengine 版本", e);
            // 非必须依赖场景：注释下面的异常，允许项目启动；必须依赖则保留
            // throw new RuntimeException("TDengine 驱动缺失，启动失败", e);
        }
//        try {
//            Class.forName(tdDriverClassName);
//            log.info("TDengine 驱动类加载成功！");
//        } catch (ClassNotFoundException e) {
//            log.info("TDengine 驱动类加载失败，请检查依赖！");
//            e.printStackTrace();
//        }
    }
    /**
     * 配置 Druid 数据源（适配 TDengine 原生驱动）
     */
//    @Bean
    @Bean(name = "tdengineDataSource")
    public DataSource tdengineDataSource() throws SQLException {
        DruidDataSource druidDataSource = new DruidDataSource();

        // 使用配置项替换硬编码的 TSDBDriver.class.getName()
        druidDataSource.setDriverClassName(tdDriverClassName);
        druidDataSource.setUrl(tdUrl);
        druidDataSource.setUsername(tdUsername);
        druidDataSource.setPassword(tdPassword);

        // Druid 连接池参数配置
        druidDataSource.setInitialSize(initialSize);
        druidDataSource.setMaxActive(maxActive);
        druidDataSource.setMinIdle(minIdle);
        druidDataSource.setMaxWait(maxWait);
        druidDataSource.setTimeBetweenEvictionRunsMillis(timeBetweenEvictionRunsMillis);
        druidDataSource.setMinEvictableIdleTimeMillis(minEvictableIdleTimeMillis);
        druidDataSource.setTestWhileIdle(testWhileIdle);
        druidDataSource.setTestOnBorrow(testOnBorrow);
        druidDataSource.setTestOnReturn(testOnReturn);

        //关闭 Druid 无效的 SQL 解析功能（消除日志干扰）    通过配置关闭 Druid 的 StatFilter（SQL 统计 / 合并）功能，避免对 TDengine 特有语法的解析报错。
        // Druid 企业级特性：开启 SQL 监控 + 防 SQL 注入
//        druidDataSource.setFilters("stat");//TDengine 不是 Druid 内置支持的数据库类型，需移除 WallFilter 配置
//        druidDataSource.setConnectionProperties("druid.stat.mergeSql=true;druid.stat.slowSqlMillis=5000");

        return druidDataSource;
    }

    /**
     * 配置 JdbcTemplate（复用原有逻辑）
     */
    @Bean(name = "tdengineJdbcTemplate")
    public JdbcTemplate tdengineJdbcTemplate(@Qualifier("tdengineDataSource") DataSource tdengineDataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(tdengineDataSource);
        jdbcTemplate.setQueryTimeout(30); // 查询超时 30秒
        return jdbcTemplate;
    }



}