package com.ruoyi.gnss.service;

import com.ruoyi.gnss.domain.GnssSolution;
import com.ruoyi.gnss.domain.NmeaRecord;
import com.ruoyi.gnss.domain.SatObservation;
import com.ruoyi.gnss.service.impl.SatelliteDataFusionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;

/**
 * GNSS 数据异步处理服务（集成队列监控）
 *
 * <p>
 * 功能说明：
 * 1. 解耦 MQTT 接收线程与数据库 I/O
 * 2. 使用生产者-消费者模式
 * 3. 批量提交数据，提高写入效率
 * 4. 集成队列监控告警
 * </p>
 *
 * @author GNSS Team
 * @date 2026-03-26
 */
@Service
public class GnssAsyncProcessor {

    private static final Logger logger = LoggerFactory.getLogger(GnssAsyncProcessor.class);

    // ==================== 配置参数 ====================

    @Value("${gnss.async.queueSize:10000}")
    private int queueSize;

    @Value("${gnss.async.batchSize:100}")
    private int batchSize;

    @Value("${gnss.async.flushIntervalMs:1000}")
    private long flushIntervalMs;

    @Value("${gnss.async.consumerThreads:2}")
    private int consumerThreads;

    @Value("${gnss.async.backpressureThreshold:0.8}")
    private double backpressureThreshold;

    @Value("${gnss.async.backpressureWaitMs:10}")
    private long backpressureWaitMs;

    @Value("${gnss.async.shutdownTimeoutSec:30}")
    private int shutdownTimeoutSec;

    @Value("${gnss.async.queueDrainTimeoutSec:10}")
    private int queueDrainTimeoutSec;

    // ==================== 依赖注入 ====================

    @Autowired(required = false)
    private IGnssStorageService gnssStorageService;

    @Autowired(required = false)
    private INmeaStorageService nmeaStorageService;

    @Autowired(required = false)
    private IRtcmStorageService rtcmStorageService;

    @Autowired(required = false)
    private ISatObservationStorageService satObservationStorageService;

    @Autowired(required = false)
    private SatelliteDataFusionService fusionService;

    @Autowired(required = false)
    private QueueMonitorService queueMonitorService;

    // ==================== 队列定义 ====================

    private BlockingQueue<NmeaTask> nmeaQueue;
    private BlockingQueue<RtcmTask> rtcmQueue;
    private BlockingQueue<SatObsTask> satObsQueue;
    private BlockingQueue<GnssSolutionTask> gnssSolutionQueue;

    // ==================== 线程池 ====================

    private ExecutorService consumerPool;
    private ScheduledExecutorService flushScheduler;

    // ==================== 背压控制 ====================

    private final AtomicBoolean backpressureActive = new AtomicBoolean(false);
    private final AtomicLong backpressureCount = new AtomicLong(0);

    // ==================== 统计信息 ====================

    private final ConcurrentHashMap<String, AtomicLong> counters = new ConcurrentHashMap<>();

    // ==================== 运行状态 ====================

    private final AtomicBoolean running = new AtomicBoolean(false);

    // ==================== 初始化 ====================

    @PostConstruct
    public void init() {
        // 初始化队列
        nmeaQueue = new LinkedBlockingQueue<>(queueSize);
        rtcmQueue = new LinkedBlockingQueue<>(queueSize);
        satObsQueue = new LinkedBlockingQueue<>(queueSize);
        gnssSolutionQueue = new LinkedBlockingQueue<>(queueSize);

        // 初始化统计计数器
        counters.put("nmea.submitted", new AtomicLong(0));
        counters.put("nmea.processed", new AtomicLong(0));
        counters.put("nmea.dropped", new AtomicLong(0));
        counters.put("rtcm.submitted", new AtomicLong(0));
        counters.put("rtcm.processed", new AtomicLong(0));
        counters.put("rtcm.dropped", new AtomicLong(0));
        counters.put("satobs.submitted", new AtomicLong(0));
        counters.put("satobs.processed", new AtomicLong(0));

        // 注册队列监控
        if (queueMonitorService != null) {
            queueMonitorService.registerQueue("NMEA", nmeaQueue, queueSize);
            queueMonitorService.registerQueue("RTCM", rtcmQueue, queueSize);
            queueMonitorService.registerQueue("SatObs", satObsQueue, queueSize);
            queueMonitorService.registerQueue("GnssSolution", gnssSolutionQueue, queueSize);

            // 添加告警监听器
            queueMonitorService.addAlertListener(this::handleQueueAlert);
        }

        // 初始化消费者线程池
        consumerPool = Executors.newFixedThreadPool(consumerThreads, r -> {
            Thread t = new Thread(r, "GNSS-Async-Consumer");
            t.setDaemon(false);
            return t;
        });

        running.set(true);

        // 启动消费者
        startConsumers();

        // 启动定时刷新任务
        flushScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "GNSS-Flush-Scheduler");
            t.setDaemon(false);
            return t;
        });
        flushScheduler.scheduleAtFixedRate(this::flushAll, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);

        logger.info("GNSS 异步处理服务初始化完成，队列大小: {}, 批量大小: {}, 消费者线程: {}, 背压阈值: {}%, 队列监控: {}",
                queueSize, batchSize, consumerThreads, (int)(backpressureThreshold * 100),
                queueMonitorService != null ? "已启用" : "未启用");
    }

    /**
     * 处理队列告警事件
     */
    private void handleQueueAlert(QueueAlertEvent event) {
        // 可以在这里添加额外的告警处理逻辑
        // 例如：发送邮件、短信、调用外部API等
        logger.info("收到队列告警: {}", event.getMessage());
    }

    // ==================== 生产者方法 ====================

    /**
     * 提交 NMEA 数据（带背压控制）
     */
    public boolean submitNmea(String nmea) {
        return submitNmea(null, nmea);
    }

    /**
     * 提交 NMEA 数据（指定站点，带背压控制）
     */
    public boolean submitNmea(String stationId, String nmea) {
        if (nmea == null || nmea.isEmpty() || !running.get()) {
            return false;
        }

        // 背压控制
        if (checkBackpressure(nmeaQueue)) {
            counters.get("nmea.dropped").incrementAndGet();
            if (queueMonitorService != null) {
                queueMonitorService.recordDrop("NMEA");
            }
            return false;
        }

        NmeaTask task = new NmeaTask(System.currentTimeMillis(), stationId, nmea);
        boolean success = nmeaQueue.offer(task);

        if (success) {
            counters.get("nmea.submitted").incrementAndGet();
            if (queueMonitorService != null) {
                queueMonitorService.recordProduce("NMEA");
            }
        } else {
            counters.get("nmea.dropped").incrementAndGet();
            if (queueMonitorService != null) {
                queueMonitorService.recordDrop("NMEA");
            }
            if (counters.get("nmea.dropped").get() % 100 == 1) {
                logger.warn("NMEA 队列已满，丢弃数据 [累计丢弃: {}]", counters.get("nmea.dropped").get());
            }
        }

        return success;
    }

    /**
     * 提交 RTCM 数据（带背压控制）
     */
    public boolean submitRtcm(byte[] rtcmData) {
        return submitRtcm(null, rtcmData);
    }

    /**
     * 提交 RTCM 数据（指定站点，带背压控制）
     */
    public boolean submitRtcm(String stationId, byte[] rtcmData) {
        if (rtcmData == null || rtcmData.length == 0 || !running.get()) {
            return false;
        }

        // 背压控制
        if (checkBackpressure(rtcmQueue)) {
            counters.get("rtcm.dropped").incrementAndGet();
            if (queueMonitorService != null) {
                queueMonitorService.recordDrop("RTCM");
            }
            return false;
        }

        RtcmTask task = new RtcmTask(System.currentTimeMillis(), stationId, rtcmData);
        boolean success = rtcmQueue.offer(task);

        if (success) {
            counters.get("rtcm.submitted").incrementAndGet();
            if (queueMonitorService != null) {
                queueMonitorService.recordProduce("RTCM");
            }
        } else {
            counters.get("rtcm.dropped").incrementAndGet();
            if (queueMonitorService != null) {
                queueMonitorService.recordDrop("RTCM");
            }
        }

        return success;
    }

    /**
     * 提交卫星观测数据
     */
    public boolean submitSatObservations(List<SatObservation> observations) {
        return submitSatObservations(null, observations);
    }

    /**
     * 提交卫星观测数据（指定站点）
     */
    public boolean submitSatObservations(String stationId, List<SatObservation> observations) {
        if (observations == null || observations.isEmpty() || !running.get()) {
            return false;
        }

        SatObsTask task = new SatObsTask(System.currentTimeMillis(), stationId, observations);
        boolean success = satObsQueue.offer(task);

        if (success) {
            counters.get("satobs.submitted").addAndGet(observations.size());
            if (queueMonitorService != null) {
                queueMonitorService.recordProduce("SatObs");
            }
        }

        return success;
    }

    /**
     * 提交 GNSS 解算结果
     */
    public boolean submitGnssSolution(GnssSolution solution) {
        if (solution == null || !running.get()) {
            return false;
        }

        GnssSolutionTask task = new GnssSolutionTask(System.currentTimeMillis(), null, solution);
        boolean success = gnssSolutionQueue.offer(task);

        if (success && queueMonitorService != null) {
            queueMonitorService.recordProduce("GnssSolution");
        }

        return success;
    }

    /**
     * 检查背压状态
     */
    private boolean checkBackpressure(BlockingQueue<?> queue) {
        int queueSize = queue.size();
        int capacity = queue.remainingCapacity() + queueSize;

        if (capacity > 0 && queueSize >= (int)(capacity * backpressureThreshold)) {
            backpressureActive.set(true);
            backpressureCount.incrementAndGet();

            // 尝试短暂等待
            if (backpressureWaitMs > 0) {
                try {
                    Thread.sleep(backpressureWaitMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            return true;
        }

        backpressureActive.set(false);
        return false;
    }

    // ==================== 消费者方法 ====================

    private void startConsumers() {
        new Thread(this::consumeNmea, "Consumer-NMEA").start();
        new Thread(this::consumeRtcm, "Consumer-RTCM").start();
        new Thread(this::consumeSatObs, "Consumer-SatObs").start();
        new Thread(this::consumeGnssSolution, "Consumer-GnssSolution").start();
    }

    /**
     * NMEA 消费者（修复批量处理逻辑）
     */
    private void consumeNmea() {
        List<NmeaTask> batch = new ArrayList<>(batchSize);
        long lastProcessTime = System.currentTimeMillis();

        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                // 阻塞等待第一个元素
                NmeaTask task = nmeaQueue.poll(100, TimeUnit.MILLISECONDS);

                if (task == null) {
                    // 队列为空，检查是否需要处理已累积的批次
                    if (!batch.isEmpty()) {
                        long now = System.currentTimeMillis();
                        // 超过刷新间隔，处理当前批次
                        if (now - lastProcessTime >= flushIntervalMs) {
                            processNmeaBatch(batch);
                            batch.clear();
                            lastProcessTime = now;
                        }
                    }
                    continue;
                }

                batch.add(task);

                // 使用 drainTo 批量获取更多元素
                int drained = nmeaQueue.drainTo(batch, batchSize - 1);

                // 达到批量大小或超时时处理
                long now = System.currentTimeMillis();
                boolean shouldProcess = batch.size() >= batchSize ||
                        (now - lastProcessTime >= flushIntervalMs && !batch.isEmpty());

                if (shouldProcess) {
                    processNmeaBatch(batch);
                    batch.clear();
                    lastProcessTime = now;
                }

            } catch (InterruptedException e) {
                logger.info("NMEA 消费者被中断");
                Thread.currentThread().interrupt();
                // 处理剩余数据
                if (!batch.isEmpty()) {
                    processNmeaBatch(batch);
                }
                break;
            } catch (Exception e) {
                logger.error("NMEA 消费异常: {}", e.getMessage());
            }
        }
    }

    /**
     * 批量处理 NMEA 数据
     */
    private void processNmeaBatch(List<NmeaTask> batch) {
        if (batch.isEmpty() || nmeaStorageService == null) {
            return;
        }

        try {
            for (NmeaTask task : batch) {
                NmeaRecord record = new NmeaRecord(new Date(task.timestamp), task.nmea);

                boolean success;
                if (task.stationId != null) {
                    success = nmeaStorageService.saveNmeaData(task.stationId, record);
                } else {
                    success = nmeaStorageService.saveNmeaData(record);
                }

                if (success) {
                    counters.get("nmea.processed").incrementAndGet();
                    if (queueMonitorService != null) {
                        queueMonitorService.recordConsume("NMEA");
                    }
                }
            }

        } catch (Exception e) {
            logger.error("批量存储 NMEA 数据失败: {}", e.getMessage());
        }
    }

    /**
     * RTCM 消费者
     */
    private void consumeRtcm() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                RtcmTask task = rtcmQueue.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }

                if (rtcmStorageService != null) {
                    rtcmStorageService.saveRtcmRawData(task.data);
                }
                counters.get("rtcm.processed").incrementAndGet();

                if (queueMonitorService != null) {
                    queueMonitorService.recordConsume("RTCM");
                }

            } catch (InterruptedException e) {
                logger.info("RTCM 消费者被中断");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("RTCM 消费异常: {}", e.getMessage());
            }
        }
    }

    /**
     * 卫星观测数据消费者
     */
    private void consumeSatObs() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                SatObsTask task = satObsQueue.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }

                if (satObservationStorageService != null) {
                    if (task.stationId != null) {
                        satObservationStorageService.saveSatObservationBatch(task.stationId, task.observations);
                    } else {
                        satObservationStorageService.saveSatObservationBatch("default", task.observations);
                    }
                }
                counters.get("satobs.processed").addAndGet(task.observations.size());

                if (queueMonitorService != null) {
                    queueMonitorService.recordConsume("SatObs");
                }

            } catch (InterruptedException e) {
                logger.info("卫星观测数据消费者被中断");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("卫星观测数据消费异常: {}", e.getMessage());
            }
        }
    }

    /**
     * GNSS 解算结果消费者
     */
    private void consumeGnssSolution() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                GnssSolutionTask task = gnssSolutionQueue.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }

                if (gnssStorageService != null) {
                    gnssStorageService.saveSolution(task.solution);
                }

                if (queueMonitorService != null) {
                    queueMonitorService.recordConsume("GnssSolution");
                }

            } catch (InterruptedException e) {
                logger.info("GNSS 解算结果消费者被中断");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("GNSS 解算结果消费异常: {}", e.getMessage());
            }
        }
    }

    // ==================== 刷新方法 ====================

    private void flushAll() {
        // 触发融合服务入库
        if (fusionService != null) {
            fusionService.flushPending();
        }
    }

    // ==================== 统计方法 ====================

    public String getStatistics() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format(
                "NMEA[submitted=%d, processed=%d, dropped=%d, pending=%d], " +
                        "RTCM[submitted=%d, processed=%d, dropped=%d, pending=%d], " +
                        "SatObs[submitted=%d, processed=%d, pending=%d], " +
                        "Backpressure[count=%d, active=%s]",
                counters.get("nmea.submitted").get(),
                counters.get("nmea.processed").get(),
                counters.get("nmea.dropped").get(),
                nmeaQueue.size(),
                counters.get("rtcm.submitted").get(),
                counters.get("rtcm.processed").get(),
                counters.get("rtcm.dropped").get(),
                rtcmQueue.size(),
                counters.get("satobs.submitted").get(),
                counters.get("satobs.processed").get(),
                satObsQueue.size(),
                backpressureCount.get(),
                backpressureActive.get()
        ));

        // 添加队列监控状态
        if (queueMonitorService != null) {
            sb.append(", ").append(queueMonitorService.getStatusSummary());
        }

        return sb.toString();
    }

    public int getNmeaQueueSize() {
        return nmeaQueue.size();
    }

    public int getRtcmQueueSize() {
        return rtcmQueue.size();
    }

    public int getSatObsQueueSize() {
        return satObsQueue.size();
    }

    public boolean isBackpressureActive() {
        return backpressureActive.get();
    }

    public double getQueueUsageRatio() {
        int totalCapacity = queueSize;
        int currentSize = nmeaQueue.size();
        return (double) currentSize / totalCapacity;
    }

    // ==================== 销毁方法 ====================

    @PreDestroy
    public void destroy() {
        logger.info("正在关闭 GNSS 异步处理服务...");

        // 1. 停止接收新任务
        running.set(false);

        // 2. 停止定时刷新任务
        flushScheduler.shutdown();

        // 3. 等待队列清空（带超时）
        waitForQueueDrain();

        // 4. 关闭消费者线程池
        consumerPool.shutdown();

        try {
            // 5. 等待消费者线程终止
            if (!consumerPool.awaitTermination(shutdownTimeoutSec, TimeUnit.SECONDS)) {
                logger.warn("消费者线程池未在 {} 秒内终止，强制关闭", shutdownTimeoutSec);
                List<Runnable> unfinishedTasks = consumerPool.shutdownNow();
                logger.warn("强制关闭后仍有 {} 个任务未执行", unfinishedTasks.size());
            }

            // 6. 等待 flushScheduler 终止
            if (!flushScheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                flushScheduler.shutdownNow();
            }

        } catch (InterruptedException e) {
            logger.warn("关闭过程被中断");
            Thread.currentThread().interrupt();
            consumerPool.shutdownNow();
            flushScheduler.shutdownNow();
        }

        // 7. 输出最终统计
        logger.info("GNSS 异步处理服务已关闭，最终统计: {}", getStatistics());
    }

    /**
     * 等待队列清空
     */
    private void waitForQueueDrain() {
        long startTime = System.currentTimeMillis();
        long timeoutMs = queueDrainTimeoutSec * 1000L;

        while (System.currentTimeMillis() - startTime < timeoutMs) {
            int nmeaPending = nmeaQueue.size();
            int rtcmPending = rtcmQueue.size();
            int satObsPending = satObsQueue.size();
            int solutionPending = gnssSolutionQueue.size();

            if (nmeaPending == 0 && rtcmPending == 0 && satObsPending == 0 && solutionPending == 0) {
                logger.info("所有队列已清空");
                return;
            }

            logger.debug("等待队列清空: NMEA={}, RTCM={}, SatObs={}, Solution={}",
                    nmeaPending, rtcmPending, satObsPending, solutionPending);

            try {
                Thread.sleep(100);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                logger.warn("等待队列清空被中断，剩余数据可能丢失");
                break;
            }
        }

        // 超时后记录剩余数据
        logger.warn("队列清空超时，剩余数据: NMEA={}, RTCM={}, SatObs={}, Solution={}",
                nmeaQueue.size(), rtcmQueue.size(), satObsQueue.size(), gnssSolutionQueue.size());
    }

    // ==================== 内部任务类 ====================

    private static class NmeaTask {
        final long timestamp;
        final String stationId;
        final String nmea;

        NmeaTask(long timestamp, String stationId, String nmea) {
            this.timestamp = timestamp;
            this.stationId = stationId;
            this.nmea = nmea;
        }
    }

    private static class RtcmTask {
        final long timestamp;
        final String stationId;
        final byte[] data;

        RtcmTask(long timestamp, String stationId, byte[] data) {
            this.timestamp = timestamp;
            this.stationId = stationId;
            this.data = data;
        }
    }

    private static class SatObsTask {
        final long timestamp;
        final String stationId;
        final List<SatObservation> observations;

        SatObsTask(long timestamp, String stationId, List<SatObservation> observations) {
            this.timestamp = timestamp;
            this.stationId = stationId;
            this.observations = observations;
        }
    }

    private static class GnssSolutionTask {
        final long timestamp;
        final String stationId;
        final GnssSolution solution;

        GnssSolutionTask(long timestamp, String stationId, GnssSolution solution) {
            this.timestamp = timestamp;
            this.stationId = stationId;
            this.solution = solution;
        }
    }
}
