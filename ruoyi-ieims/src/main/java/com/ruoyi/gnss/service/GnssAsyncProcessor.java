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

    @Value("${gnss.async.backpressureWaitMs:10}")
    private long backpressureWaitMs;

    @Value("${gnss.async.offerTimeoutMs:50}")
    private long offerTimeoutMs;

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
        nmeaQueue = new LinkedBlockingQueue<>(queueSize);
        rtcmQueue = new LinkedBlockingQueue<>(queueSize);
        satObsQueue = new LinkedBlockingQueue<>(queueSize);
        gnssSolutionQueue = new LinkedBlockingQueue<>(queueSize);

        counters.put("nmea.submitted", new AtomicLong(0));
        counters.put("nmea.processed", new AtomicLong(0));
        counters.put("nmea.dropped", new AtomicLong(0));
        counters.put("rtcm.submitted", new AtomicLong(0));
        counters.put("rtcm.processed", new AtomicLong(0));
        counters.put("rtcm.dropped", new AtomicLong(0));
        counters.put("satobs.submitted", new AtomicLong(0));
        counters.put("satobs.processed", new AtomicLong(0));
        counters.put("satobs.dropped", new AtomicLong(0));
        counters.put("solution.submitted", new AtomicLong(0));
        counters.put("solution.processed", new AtomicLong(0));
        counters.put("solution.dropped", new AtomicLong(0));

        if (queueMonitorService != null) {
            queueMonitorService.registerQueue("NMEA", nmeaQueue, queueSize);
            queueMonitorService.registerQueue("RTCM", rtcmQueue, queueSize);
            queueMonitorService.registerQueue("SatObs", satObsQueue, queueSize);
            queueMonitorService.registerQueue("GnssSolution", gnssSolutionQueue, queueSize);
            queueMonitorService.addAlertListener(this::handleQueueAlert);
        }

        consumerPool = Executors.newFixedThreadPool(consumerThreads, r -> {
            Thread t = new Thread(r, "GNSS-Async-Consumer");
            t.setDaemon(false);
            return t;
        });

        running.set(true);
        startConsumers();

        flushScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "GNSS-Flush-Scheduler");
            t.setDaemon(false);
            return t;
        });
        flushScheduler.scheduleAtFixedRate(this::flushAll, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);

        logger.info("GNSS 异步处理服务初始化完成，队列大小: {}, 消费者线程: {}, 背压阈值: {}%",
                queueSize, consumerThreads, (int)(backpressureThreshold * 100));
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

        // 1. 触发背压限流（生产端减速，但不丢弃）
        applyBackpressure(nmeaQueue);

        NmeaTask task = new NmeaTask(System.currentTimeMillis(), safeStationId, nmea);

        // 2. 弹性入队：如果满了，等待 offerTimeoutMs 毫秒
        boolean success = false;
        try {
            success = nmeaQueue.offer(task, offerTimeoutMs, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        if (success) {
            counters.get("nmea.submitted").incrementAndGet();
            if (queueMonitorService != null) queueMonitorService.recordProduce("NMEA");
        } else {
            // 只有 100% 满且等待超时后，才真正丢弃
            counters.get("nmea.dropped").incrementAndGet();
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
            counters.get("rtcm.submitted").incrementAndGet();
            if (queueMonitorService != null) queueMonitorService.recordProduce("RTCM");
        } else {
            counters.get("rtcm.dropped").incrementAndGet();
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
            counters.get("satobs.submitted").addAndGet(observations.size());
            if (queueMonitorService != null) queueMonitorService.recordProduce("SatObs");
        } else {
            counters.get("satobs.dropped").addAndGet(observations.size());
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
            counters.get("solution.submitted").incrementAndGet();
            if (queueMonitorService != null) queueMonitorService.recordProduce("GnssSolution");
        } else {
            counters.get("solution.dropped").incrementAndGet();
            if (queueMonitorService != null) queueMonitorService.recordDrop("GnssSolution");
        }
        return success;
    }

    /**
     * 【修复】真正的背压逻辑：只减速，不干预丢弃逻辑
     */
    private void applyBackpressure(BlockingQueue<?> queue) {
        int queueSize = queue.size();
        int capacity = queue.remainingCapacity() + queueSize;

        if (capacity > 0 && queueSize >= (int)(capacity * backpressureThreshold)) {
            backpressureActive.set(true);
            backpressureCount.incrementAndGet();
            if (backpressureWaitMs > 0) {
                try {
                    // 强行让 MQTT 接收线程睡一会儿，降低洪水灌入速度
                    Thread.sleep(backpressureWaitMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        } else {
            backpressureActive.set(false);
        }
    }

    // ==================== 消费者方法 ====================

    private void startConsumers() {
        new Thread(this::consumeNmea, "Consumer-NMEA").start();
        new Thread(this::consumeRtcm, "Consumer-RTCM").start();
        new Thread(this::consumeSatObs, "Consumer-SatObs").start();
        new Thread(this::consumeGnssSolution, "Consumer-GnssSolution").start();
    }

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
        if (batch.isEmpty() || nmeaStorageService == null) return;
        try {
            java.util.Map<String, List<NmeaRecord>> byStation = new java.util.HashMap<>();

            for (NmeaTask task : batch) {
                NmeaRecord record = new NmeaRecord(new Date(task.timestamp), task.nmea);
                String sid = (task.stationId != null) ? task.stationId : StationContext.getDefaultStationId();
                byStation.computeIfAbsent(sid, k -> new java.util.ArrayList<>()).add(record);
            }
            // 批量提交每个站点的聚合数据
            for (java.util.Map.Entry<String, List<NmeaRecord>> entry : byStation.entrySet()) {
                int saved = nmeaStorageService.saveNmeaBatch(entry.getKey(), entry.getValue());

                if (saved > 0) {
                    counters.get("nmea.processed").addAndGet(saved);
                    // 批量更新监控大屏计数
                    if (queueMonitorService != null) {
                        for(int i = 0; i < saved; i++) {
                            queueMonitorService.recordConsume("NMEA");
                        }
                    }
                }
            }
        } catch (Exception e) {
            logger.error("批量存储 NMEA 数据失败: {}", e.getMessage());
        }
    }

    private void consumeRtcm() {
        while (running.get() && !Thread.currentThread().isInterrupted()) {
            try {
                RtcmTask task = rtcmQueue.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) continue;

                if (rtcmStorageService != null) {
                    String sid = (task.stationId != null) ? task.stationId : StationContext.getDefaultStationId();
                    rtcmStorageService.saveRtcmRawData(sid, task.data);
                }
                counters.get("rtcm.processed").incrementAndGet();
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

                if (satObservationStorageService != null) {
                    String sid = (task.stationId != null) ? task.stationId : StationContext.getDefaultStationId();
                    satObservationStorageService.saveSatObservationBatch(sid, task.observations);
                }
                counters.get("satobs.processed").addAndGet(task.observations.size());
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

                if (gnssStorageService != null) {
                    String sid = (task.stationId != null) ? task.stationId : StationContext.getDefaultStationId();
                    gnssStorageService.saveSolution(sid, task.solution);
                }
                counters.get("solution.processed").incrementAndGet();
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
                counters.get("nmea.submitted").get(), counters.get("nmea.processed").get(), counters.get("nmea.dropped").get(), nmeaQueue.size(),
                counters.get("rtcm.submitted").get(), counters.get("rtcm.processed").get(), counters.get("rtcm.dropped").get(), rtcmQueue.size(),
                counters.get("satobs.submitted").get(), counters.get("satobs.processed").get(), counters.get("satobs.dropped").get(), satObsQueue.size(),
                counters.get("solution.submitted").get(), counters.get("solution.processed").get(), counters.get("solution.dropped").get(), gnssSolutionQueue.size(),
                backpressureCount.get(), backpressureActive.get()
        ));
        if (queueMonitorService != null) sb.append(", ").append(queueMonitorService.getStatusSummary());
        return sb.toString();
    }

    @PreDestroy
    public void destroy() {
        logger.info("正在关闭 GNSS 异步处理服务...");
        running.set(false);
        flushScheduler.shutdown();

        long startTime = System.currentTimeMillis();
        while (System.currentTimeMillis() - startTime < queueDrainTimeoutSec * 1000L) {
            if (nmeaQueue.isEmpty() && rtcmQueue.isEmpty() && satObsQueue.isEmpty() && gnssSolutionQueue.isEmpty()) break;
            try { Thread.sleep(100); } catch (InterruptedException e) { break; }
        }

        consumerPool.shutdown();
        logger.info("GNSS 异步处理服务已关闭，最终统计: {}", getStatistics());
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