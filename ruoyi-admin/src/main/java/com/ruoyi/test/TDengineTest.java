package com.ruoyi.test;

import com.ruoyi.user.comm.core.redis.RedisUtil;
import com.ruoyi.user.comm.core.tdengine.TDengineUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

@Component // 标记为Spring组件，交由容器管理
@Slf4j
public class TDengineTest {

    // ==================== 依赖注入 ====================
    @Autowired
    private TDengineUtil tDengineUtil;

    @Autowired
    private ThreadPoolTaskExecutor taskExecutor;

    @Autowired
    private IonosphereDataService ionosphereDataService;

    /**
     * 原有方法：启动后延迟3秒执行，固定间隔1天（仅执行一次）
     * fixedDelay = 86400000 表示执行完一次后，间隔1天再执行
     */
//    @Scheduled(initialDelay = 3000, fixedDelay = 86400000)
    public void targetMethod2() {
        // 核心优化：通过线程池异步执行任务，避免阻塞定时任务线程池
        taskExecutor.execute(() -> {
            try {
                // 调用核心业务方法（异步执行，不阻塞定时任务触发）
                ionosphereDataService.testConnection();
                // 生成随机数据插入（改用ThreadLocalRandom，线程安全）
                insertRandomIonData(1, "桂林");
                // 插入不含温度的数据
                insertRandomIonDataWithoutTemp(2, "桂林");

            } catch (Exception e) {
                // 捕获任务执行异常，打印错误日志
                log.error("【targetMethod2】异步任务执行失败", e);
            }
        });
    }

    /**
     * 新增方法：每秒执行一次（固定速率）
     * fixedRate = 1000 表示每1000毫秒（1秒）执行一次，不受任务执行耗时影响
     * 注意：需保证任务执行耗时 < 1秒，避免任务堆积；若耗时可能超1秒，建议改用异步执行
     */
//    @Scheduled(fixedRate = 1000)
    @Scheduled(initialDelay = 3000, fixedDelay = 86400000)
    public void perSecondTask() {
        // 异步执行每秒任务，避免阻塞定时任务线程
        taskExecutor.execute(() -> {
            try {
                long currentTime = System.currentTimeMillis();
                // 每秒生成一条随机数据插入TDengine
                int stationId = ThreadLocalRandom.current().nextInt(1, 5); // 随机站点ID 1-4
                String region = "桂林";
                float signalStrength = ThreadLocalRandom.current().nextFloat() * 50 + 50; // 50-100
                int electronDensity = ThreadLocalRandom.current().nextInt(10) + 20; // 20-30
                Float temperature = ThreadLocalRandom.current().nextBoolean() ?
                        ThreadLocalRandom.current().nextFloat() * 20 + 10 : null; // 10-30 或 null

                // 插入数据
                ionosphereDataService.insertIonDataWithTemperature(
                        stationId, region, currentTime, signalStrength, electronDensity, temperature
                );
                log.info("【perSecondTask】每秒任务执行成功，站点ID：{}，时间戳：{}", stationId, currentTime);
            } catch (Exception e) {
                log.error("【perSecondTask】每秒任务执行失败", e);
            }
        });
    }

    // ==================== 工具方法（简化代码） ====================
    /**
     * 插入含温度的随机电离层数据
     */
    private void insertRandomIonData(int stationId, String region) {
        long ts = new Date().getTime();
        float signalStrength = ThreadLocalRandom.current().nextFloat() * 50 + 50;
        int electronDensity = ThreadLocalRandom.current().nextInt(10) + 20;
        float temperature = ThreadLocalRandom.current().nextFloat() * 20 + 10;
        ionosphereDataService.insertIonDataWithTemperature(stationId, region, ts, signalStrength, electronDensity, temperature);
    }

    /**
     * 插入不含温度的随机电离层数据
     */
    private void insertRandomIonDataWithoutTemp(int stationId, String region) {
        long ts = new Date().getTime();
        float signalStrength = ThreadLocalRandom.current().nextFloat() * 50 + 50;
        int electronDensity = ThreadLocalRandom.current().nextInt(10) + 20;
        ionosphereDataService.insertIonDataWithTemperature(stationId, region, ts, signalStrength, electronDensity, null);
    }
}