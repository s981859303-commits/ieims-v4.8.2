package com.ruoyi.ieims.gnss.service;

import com.ruoyi.ieims.gnss.domain.GnssSolution;
import com.ruoyi.ieims.gnss.domain.SatObservation;
import com.ruoyi.ieims.gnss.service.impl.SatelliteDataFusionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * GNSS 数据异步处理服务
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

    @Value("${gnss.async.offerTimeoutMs:50}")
    private long offerTimeoutMs;

    @Value("${gnss.async.queueDrainTimeoutSec:10}")
    private int queueDrainTimeoutSec;

    // ==================== 依赖注入 ====================

    @Autowired(required = false)
    private SatelliteDataFusionService fusionService;

    @Autowired(required = false)
    private QueueMonitorService queueMonitorService;

    @Autowired(required = false)
    private com.ruoyi.user.comm.core.mqtt.MqttUtil mqttUtil;

    @Autowired(required = false)
    private RawDataRecordService rawDataRecordService;

    // ==================== 队列定义 ====================

    private BlockingQueue<NmeaTask> nmeaQueue;
    private BlockingQueue<RtcmTask> rtcmQueue;
    private BlockingQueue<SatObsTask> satObsQueue;
    private BlockingQueue<GnssSolutionTask> gnssSolutionQueue;

    // ==================== 线程池 ====================

    private ScheduledExecutorService flushScheduler;
    private ExecutorService consumerPool;

    // ==================== 背压与运行状态 ====================

    private final AtomicBoolean backpressureActive = new AtomicBoolean(false);
    private final AtomicLong backpressureCount = new AtomicLong(0);
    private final AtomicBoolean running = new AtomicBoolean(false);

    // ==================== 统计变量  ====================

    private final AtomicLong nmeaSubCount = new AtomicLong(0);
    private final AtomicLong nmeaProCount = new AtomicLong(0);
    private final AtomicLong nmeaDropCount = new AtomicLong(0);

    private final AtomicLong rtcmSubCount = new AtomicLong(0);
    private final AtomicLong rtcmProCount = new AtomicLong(0);
    private final AtomicLong rtcmDropCount = new AtomicLong(0);

    private final AtomicLong satObsSubCount = new AtomicLong(0);
    private final AtomicLong satObsProCount = new AtomicLong(0);
    private final AtomicLong satObsDropCount = new AtomicLong(0);

    private final AtomicLong solutionSubCount = new AtomicLong(0);
    private final AtomicLong solutionProCount = new AtomicLong(0);
    private final AtomicLong solutionDropCount = new AtomicLong(0);

    // ==================== 初始化 ====================

    @PostConstruct
    public void init() {
        nmeaQueue = new LinkedBlockingQueue<>(queueSize);
        rtcmQueue = new LinkedBlockingQueue<>(queueSize);
        satObsQueue = new LinkedBlockingQueue<>(queueSize);
        gnssSolutionQueue = new LinkedBlockingQueue<>(queueSize);

        if (queueMonitorService != null) {
            queueMonitorService.registerQueue("NMEA", nmeaQueue, queueSize);
            queueMonitorService.registerQueue("RTCM", rtcmQueue, queueSize);
            queueMonitorService.registerQueue("SatObs", satObsQueue, queueSize);
            queueMonitorService.registerQueue("GnssSolution", gnssSolutionQueue, queueSize);
            queueMonitorService.addAlertListener(this::handleQueueAlert);
        }

        // 1. 初始化定时刷新的 Scheduler
        flushScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "GNSS-Flush-Scheduler");
            t.setDaemon(false);
            return t;
        });
        flushScheduler.scheduleAtFixedRate(this::flushAll, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);

        logger.info("GNSS 异步处理服务初始化完成，队列大小: {}, 消费者线程(JSON序列化): {}, 背压阈值: {}%",
                queueSize, consumerThreads, (int)(backpressureThreshold * 100));

        // 2. 动态计算并初始化消费者线程池
        int satObsWorkers = Math.max(2, consumerThreads);
        int totalWorkers = 3 + satObsWorkers; // NMEA(1) + RTCM(1) + Solution(1) + SatObs(N)

        consumerPool = Executors.newFixedThreadPool(totalWorkers, r -> {
            Thread t = new Thread(r);
            t.setName("GNSS-Worker-" + t.getId());
            t.setDaemon(false);
            return t;
        });

        // 3. 标记运行并启动所有消费者
        running.set(true);
        startConsumers();
    }

    private void startConsumers() {
        // 提交轻量级任务（各分配 1 个独立线程足以应对）
        consumerPool.submit(this::consumeNmea);
        consumerPool.submit(this::consumeRtcm);
        consumerPool.submit(this::consumeGnssSolution);

        // 提交重量级任务：动态分配多个线程并发抢占消费 SatObs (分摊高强度的 JSON 序列化压力)
        int satObsWorkers = Math.max(2, consumerThreads);
        for (int i = 0; i < satObsWorkers; i++) {
            consumerPool.submit(this::consumeSatObs);
        }
    }

    @PreDestroy
    public void destroy() {
        logger.info("正在关闭 GNSS 异步处理服务...");
        running.set(false);
        flushScheduler.shutdown();

        // 等待队列清空
        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < queueDrainTimeoutSec * 1000L) {
            if (nmeaQueue.isEmpty() && rtcmQueue.isEmpty() && satObsQueue.isEmpty() && gnssSolutionQueue.isEmpty()) break;
            try { Thread.sleep(100); } catch (InterruptedException e) { break; }
        }

        // 彻底且安全地关闭所有工作线程
        if (consumerPool != null) {
            consumerPool.shutdownNow();
        }

        logger.info("GNSS 异步处理服务已关闭，最终统计: {}", getStatistics());
    }

    private void handleQueueAlert(QueueAlertEvent event) {
        logger.info("收到队列告警: {}", event.getMessage());
    }

    // ==================== 生产者方法 ====================

    public boolean submitNmea(String nmea) {
        return submitNmea(StationContext.getCurrentStationId(), nmea);
    }

    public boolean submitNmea(String stationId, String nmea) {
        if (nmea == null || nmea.isEmpty() || !running.get()) return false;

        String safeStationId = (stationId != null) ? stationId : StationContext.getDefaultStationId();
        applyBackpressure(nmeaQueue);

        NmeaTask task = new NmeaTask(System.currentTimeMillis(), safeStationId, nmea);
        boolean success = false;
        try {
            success = nmeaQueue.offer(task, offerTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (success) {
            nmeaSubCount.incrementAndGet();
            if (queueMonitorService != null) queueMonitorService.recordProduce("NMEA");
        } else {
            nmeaDropCount.incrementAndGet();
            if (queueMonitorService != null) queueMonitorService.recordDrop("NMEA");
        }
        return success;
    }

    public boolean submitRtcm(byte[] rtcmData) {
        return submitRtcm(StationContext.getCurrentStationId(), rtcmData);
    }

    public boolean submitRtcm(String stationId, byte[] rtcmData) {
        if (rtcmData == null || rtcmData.length == 0 || !running.get()) return false;

        String safeStationId = (stationId != null) ? stationId : StationContext.getDefaultStationId();
        applyBackpressure(rtcmQueue);

        RtcmTask task = new RtcmTask(System.currentTimeMillis(), safeStationId, rtcmData);
        boolean success = false;
        try {
            success = rtcmQueue.offer(task, offerTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (success) {
            rtcmSubCount.incrementAndGet();
            if (queueMonitorService != null) queueMonitorService.recordProduce("RTCM");
        } else {
            rtcmDropCount.incrementAndGet();
            if (queueMonitorService != null) queueMonitorService.recordDrop("RTCM");
        }
        return success;
    }

    public boolean submitSatObservations(List<SatObservation> observations) {
        return submitSatObservations(StationContext.getCurrentStationId(), observations);
    }

    public boolean submitSatObservations(String stationId, List<SatObservation> observations) {
        if (observations == null || observations.isEmpty() || !running.get()) return false;

        String safeStationId = (stationId != null) ? stationId : StationContext.getDefaultStationId();
        applyBackpressure(satObsQueue);

        SatObsTask task = new SatObsTask(System.currentTimeMillis(), safeStationId, observations);
        boolean success = false;
        try {
            success = satObsQueue.offer(task, offerTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (success) {
            satObsSubCount.addAndGet(observations.size());
            if (queueMonitorService != null) queueMonitorService.recordProduce("SatObs");
        } else {
            satObsDropCount.addAndGet(observations.size());
            if (queueMonitorService != null) queueMonitorService.recordDrop("SatObs");
        }
        return success;
    }

    public boolean submitGnssSolution(GnssSolution solution) {
        return submitGnssSolution(StationContext.getCurrentStationId(), solution);
    }

    public boolean submitGnssSolution(String stationId, GnssSolution solution) {
        if (solution == null || !running.get()) return false;

        String safeStationId = (stationId != null) ? stationId : StationContext.getDefaultStationId();
        applyBackpressure(gnssSolutionQueue);

        GnssSolutionTask task = new GnssSolutionTask(System.currentTimeMillis(), safeStationId, solution);
        boolean success = false;
        try {
            success = gnssSolutionQueue.offer(task, offerTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (success) {
            solutionSubCount.incrementAndGet();
            if (queueMonitorService != null) queueMonitorService.recordProduce("GnssSolution");
        } else {
            solutionDropCount.incrementAndGet();
            if (queueMonitorService != null) queueMonitorService.recordDrop("GnssSolution");
        }
        return success;
    }

    /**
     * 真正的背压逻辑：只减速（利用 offer 的超时特性），不强行休眠中断网络 IO
     */
    private void applyBackpressure(BlockingQueue<?> queue) {
        int qSize = queue.size();
        int capacity = queue.remainingCapacity() + qSize;

        if (capacity > 0 && qSize >= (int)(capacity * backpressureThreshold)) {
            backpressureActive.set(true);
            backpressureCount.incrementAndGet();
            // 不再调用 Thread.sleep()！依靠底层 queue.offer(timeout) 自然限流
        } else {
            backpressureActive.set(false);
        }
    }

    // ==================== 消费者方法 ====================

    private void consumeNmea() {
        List<NmeaTask> batch = new ArrayList<>(batchSize);
        long lastProcessTime = System.currentTimeMillis();

        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                NmeaTask task = nmeaQueue.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) {
                    if (!batch.isEmpty()) {
                        long now = System.currentTimeMillis();
                        if (now - lastProcessTime >= flushIntervalMs) {
                            processNmeaBatch(batch);
                            batch.clear();
                            lastProcessTime = now;
                        }
                    }
                    continue;
                }
                batch.add(task);
                nmeaQueue.drainTo(batch, batchSize - 1);

                long now = System.currentTimeMillis();
                if (batch.size() >= batchSize || (now - lastProcessTime >= flushIntervalMs)) {
                    processNmeaBatch(batch);
                    batch.clear();
                    lastProcessTime = now;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                if (!batch.isEmpty()) processNmeaBatch(batch);
                break;
            } catch (Throwable t) {
                logger.error("NMEA 消费异常: {}", t.getMessage());
            }
        }
    }

    private void processNmeaBatch(List<NmeaTask> batch) {
        if (batch.isEmpty()) return;
        try {
            int processedCount = batch.size();
            nmeaProCount.addAndGet(processedCount);

            // ➕【新增】：批量落盘 NMEA 数据
            if (rawDataRecordService != null) {
                for (NmeaTask task : batch) {
                    rawDataRecordService.recordNmea(task.stationId, task.nmea);
                }
            }

            if (queueMonitorService != null) {
                for(int i = 0; i < processedCount; i++) {
                    queueMonitorService.recordConsume("NMEA");
                }
            }
        } catch (Exception e) {
            logger.error("处理 NMEA 批量数据异常: {}", e.getMessage());
        }
    }

    private void consumeRtcm() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                RtcmTask task = rtcmQueue.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) continue;

                if (rawDataRecordService != null) {
                    rawDataRecordService.recordRtcm(task.stationId, task.data);
                }
                // ==========================================================

                rtcmProCount.incrementAndGet();
                if (queueMonitorService != null) queueMonitorService.recordConsume("RTCM");

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Throwable t) {
                logger.error("RTCM 消费异常: {}", t.getMessage());
            }
        }
    }

    private void consumeSatObs() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                SatObsTask task = satObsQueue.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) continue;

                String sid = (task.stationId != null) ? task.stationId : StationContext.getDefaultStationId();

                if (mqttUtil != null && task.observations != null) {
                    try {
                        String targetTopic = "ieims/gnss/obs/" + sid;

                        // 【安全过滤】剔除历元时间为 0 的无效缓冲帧
                        List<SatObservation> validMqttList = task.observations.stream()
                                .filter(obs -> {
                                    boolean isZeroEpochRtcm = SatObservation.SOURCE_RTCM.equals(obs.getDataSource())
                                            && (obs.getEpochTime() == null || obs.getEpochTime() == 0L);
                                    return !isZeroEpochRtcm;
                                })
                                .collect(java.util.stream.Collectors.toList());

                        if (!validMqttList.isEmpty()) {
                            String payload = com.alibaba.fastjson.JSON.toJSONString(validMqttList);
                            mqttUtil.publish(targetTopic, payload,0, false);
                        }

                    } catch (Exception e) {
                        logger.error("向内网 MQTT 发布卫星观测数据失败: {}", e.getMessage());
                    }
                }

                satObsProCount.addAndGet(task.observations.size());
                if (queueMonitorService != null) queueMonitorService.recordConsume("SatObs");

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Throwable t) {
                logger.error("卫星观测数据消费异常: {}", t.getMessage());
            }
        }
    }


    private void consumeGnssSolution() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                GnssSolutionTask task = gnssSolutionQueue.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) continue;

                String sid = (task.stationId != null) ? task.stationId : StationContext.getDefaultStationId();

                if (task.solution != null) {
                    task.solution.setStationId(sid);
                }

                if (mqttUtil != null) {
                    try {
                        String targetTopic = "ieims/gnss/solution/" + sid;
                        String payload = com.alibaba.fastjson.JSON.toJSONString(task.solution);
                        mqttUtil.publish(targetTopic, payload, 0, false);
                    } catch (Exception e) {
                        logger.error("向内网 MQTT 发布解算结果失败: {}", e.getMessage());
                    }
                }

                solutionProCount.incrementAndGet();
                if (queueMonitorService != null) queueMonitorService.recordConsume("GnssSolution");

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Throwable t) {
                logger.error("GNSS 解算结果消费异常: {}", t.getMessage());
            }
        }
    }

    private void flushAll() {
        if (fusionService != null) {
            fusionService.flushPending();
        }
    }

    // ==================== 统计方法 ====================

    public String getStatistics() {
        StringBuilder sb = new StringBuilder();
        sb.append(String.format(
                "NMEA[sub=%d, pro=%d, drop=%d, pend=%d], " +
                        "RTCM[sub=%d, pro=%d, drop=%d, pend=%d], " +
                        "SatObs[sub=%d, pro=%d, drop=%d, pend=%d], " +
                        "Solution[sub=%d, pro=%d, drop=%d, pend=%d], " +
                        "Backpressure[cnt=%d, act=%s]",
                nmeaSubCount.get(), nmeaProCount.get(), nmeaDropCount.get(), nmeaQueue.size(),
                rtcmSubCount.get(), rtcmProCount.get(), rtcmDropCount.get(), rtcmQueue.size(),
                satObsSubCount.get(), satObsProCount.get(), satObsDropCount.get(), satObsQueue.size(),
                solutionSubCount.get(), solutionProCount.get(), solutionDropCount.get(), gnssSolutionQueue.size(),
                backpressureCount.get(), backpressureActive.get()
        ));
        if (queueMonitorService != null) sb.append(", ").append(queueMonitorService.getStatusSummary());
        return sb.toString();
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