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
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.ConcurrentHashMap;

/**
 * GNSS 数据异步处理服务
 *
 * <p>
 * 功能说明：
 * 1. 解耦 MQTT 接收线程与数据库 I/O
 * 2. 使用生产者-消费者模式
 * 3. 批量提交数据，提高写入效率
 * </p>
 *
 * @author GNSS Team
 * @date 2026-03-25
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

    // ==================== 队列定义 ====================

    /** NMEA 数据队列 */
    private BlockingQueue<NmeaTask> nmeaQueue;

    /** RTCM 数据队列 */
    private BlockingQueue<RtcmTask> rtcmQueue;

    /** 卫星观测数据队列 */
    private BlockingQueue<SatObsTask> satObsQueue;

    /** GNSS 解算结果队列 */
    private BlockingQueue<GnssSolutionTask> gnssSolutionQueue;

    // ==================== 线程池 ====================

    private ExecutorService consumerPool;
    private ScheduledExecutorService flushScheduler;

    // ==================== 统计信息 ====================

    private final ConcurrentHashMap<String, AtomicLong> counters = new ConcurrentHashMap<>();

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
        counters.put("rtcm.submitted", new AtomicLong(0));
        counters.put("rtcm.processed", new AtomicLong(0));
        counters.put("satobs.submitted", new AtomicLong(0));
        counters.put("satobs.processed", new AtomicLong(0));

        // 初始化消费者线程池
        consumerPool = Executors.newFixedThreadPool(consumerThreads, r -> {
            Thread t = new Thread(r, "GNSS-Async-Consumer");
            t.setDaemon(true);
            return t;
        });

        // 启动消费者
        startConsumers();

        // 启动定时刷新任务
        flushScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "GNSS-Flush-Scheduler");
            t.setDaemon(true);
            return t;
        });
        flushScheduler.scheduleAtFixedRate(this::flushAll, flushIntervalMs, flushIntervalMs, TimeUnit.MILLISECONDS);

        logger.info("GNSS 异步处理服务初始化完成，队列大小: {}, 批量大小: {}, 消费者线程: {}",
                queueSize, batchSize, consumerThreads);
    }

    // ==================== 生产者方法 ====================

    /**
     * 提交 NMEA 数据（非阻塞）
     *
     * @param nmea NMEA 语句
     * @return true 表示提交成功
     */
    public boolean submitNmea(String nmea) {
        if (nmea == null || nmea.isEmpty()) {
            return false;
        }

        NmeaTask task = new NmeaTask(System.currentTimeMillis(), nmea);
        boolean success = nmeaQueue.offer(task);  // 非阻塞

        if (success) {
            counters.get("nmea.submitted").incrementAndGet();
        } else {
            logger.warn("NMEA 队列已满，丢弃数据");
        }

        return success;
    }

    /**
     * 提交 RTCM 数据（非阻塞）
     *
     * @param rtcmData RTCM 二进制数据
     * @return true 表示提交成功
     */
    public boolean submitRtcm(byte[] rtcmData) {
        if (rtcmData == null || rtcmData.length == 0) {
            return false;
        }

        RtcmTask task = new RtcmTask(System.currentTimeMillis(), rtcmData);
        boolean success = rtcmQueue.offer(task);  // 非阻塞

        if (success) {
            counters.get("rtcm.submitted").incrementAndGet();
        } else {
            logger.warn("RTCM 队列已满，丢弃数据");
        }

        return success;
    }

    /**
     * 提交卫星观测数据（非阻塞）
     *
     * @param observations 卫星观测数据列表
     * @return true 表示提交成功
     */
    public boolean submitSatObservations(List<SatObservation> observations) {
        if (observations == null || observations.isEmpty()) {
            return false;
        }

        SatObsTask task = new SatObsTask(System.currentTimeMillis(), observations);
        boolean success = satObsQueue.offer(task);  // 非阻塞

        if (success) {
            counters.get("satobs.submitted").addAndGet(observations.size());
        } else {
            logger.warn("卫星观测数据队列已满，丢弃数据");
        }

        return success;
    }

    /**
     * 提交 GNSS 解算结果（非阻塞）
     *
     * @param solution GNSS 解算结果
     * @return true 表示提交成功
     */
    public boolean submitGnssSolution(GnssSolution solution) {
        if (solution == null) {
            return false;
        }

        GnssSolutionTask task = new GnssSolutionTask(System.currentTimeMillis(), solution);
        return gnssSolutionQueue.offer(task);
    }

    // ==================== 消费者方法 ====================

    private void startConsumers() {
        // NMEA 消费者
        consumerPool.submit(this::consumeNmea);

        // RTCM 消费者
        consumerPool.submit(this::consumeRtcm);

        // 卫星观测数据消费者
        consumerPool.submit(this::consumeSatObs);

        // GNSS 解算结果消费者
        consumerPool.submit(this::consumeGnssSolution);
    }

    private void consumeNmea() {
        List<NmeaTask> batch = new ArrayList<>(batchSize);

        while (!Thread.currentThread().isInterrupted()) {
            try {
                // 阻塞等待第一个元素
                NmeaTask task = nmeaQueue.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }

                batch.add(task);

                // 尝试获取更多元素（批量处理）
                nmeaQueue.drainTo(new ArrayList<>(), batchSize - 1);
                for (int i = 0; i < batchSize - 1 && !nmeaQueue.isEmpty(); i++) {
                    NmeaTask t = nmeaQueue.poll();
                    if (t != null) {
                        batch.add(t);
                    }
                }

                // 处理批量数据
                processNmeaBatch(batch);

                batch.clear();

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("NMEA 消费异常: {}", e.getMessage());
            }
        }
    }

    private void processNmeaBatch(List<NmeaTask> batch) {
        if (batch.isEmpty() || nmeaStorageService == null) {
            return;
        }

        try {
            for (NmeaTask task : batch) {
                NmeaRecord record = new NmeaRecord(new Date(task.timestamp), task.nmea);
                nmeaStorageService.saveNmeaData(record);
            }
            counters.get("nmea.processed").addAndGet(batch.size());
        } catch (Exception e) {
            logger.error("批量存储 NMEA 数据失败: {}", e.getMessage());
        }
    }

    private void consumeRtcm() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                RtcmTask task = rtcmQueue.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }

                if (rtcmStorageService != null) {
                    rtcmStorageService.saveRtcmRawData(task.data);
                }
                counters.get("rtcm.processed").incrementAndGet();

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("RTCM 消费异常: {}", e.getMessage());
            }
        }
    }

    private void consumeSatObs() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                SatObsTask task = satObsQueue.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }

                if (satObservationStorageService != null) {
                    satObservationStorageService.saveSatObservationBatch(task.observations);
                }
                counters.get("satobs.processed").addAndGet(task.observations.size());

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("卫星观测数据消费异常: {}", e.getMessage());
            }
        }
    }

    private void consumeGnssSolution() {
        while (!Thread.currentThread().isInterrupted()) {
            try {
                GnssSolutionTask task = gnssSolutionQueue.poll(100, TimeUnit.MILLISECONDS);
                if (task == null) {
                    continue;
                }

                if (gnssStorageService != null) {
                    gnssStorageService.saveSolution(task.solution);
                }

            } catch (InterruptedException e) {
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
            fusionService.fuseAndStore();
        }
    }

    // ==================== 统计方法 ====================

    public String getStatistics() {
        return String.format(
                "NMEA[submitted=%d, processed=%d, pending=%d], " +
                        "RTCM[submitted=%d, processed=%d, pending=%d], " +
                        "SatObs[submitted=%d, processed=%d, pending=%d]",
                counters.get("nmea.submitted").get(),
                counters.get("nmea.processed").get(),
                nmeaQueue.size(),
                counters.get("rtcm.submitted").get(),
                counters.get("rtcm.processed").get(),
                rtcmQueue.size(),
                counters.get("satobs.submitted").get(),
                counters.get("satobs.processed").get(),
                satObsQueue.size()
        );
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

    // ==================== 销毁方法 ====================

    @PreDestroy
    public void destroy() {
        logger.info("正在关闭 GNSS 异步处理服务...");

        // 停止接收新任务
        flushScheduler.shutdown();
        consumerPool.shutdown();

        try {
            // 等待消费者处理完剩余数据
            if (!consumerPool.awaitTermination(30, TimeUnit.SECONDS)) {
                consumerPool.shutdownNow();
            }
        } catch (InterruptedException e) {
            consumerPool.shutdownNow();
        }

        logger.info("GNSS 异步处理服务已关闭，最终统计: {}", getStatistics());
    }

    // ==================== 内部任务类 ====================

    private static class NmeaTask {
        final long timestamp;
        final String nmea;

        NmeaTask(long timestamp, String nmea) {
            this.timestamp = timestamp;
            this.nmea = nmea;
        }
    }

    private static class RtcmTask {
        final long timestamp;
        final byte[] data;

        RtcmTask(long timestamp, byte[] data) {
            this.timestamp = timestamp;
            this.data = data;
        }
    }

    private static class SatObsTask {
        final long timestamp;
        final List<SatObservation> observations;

        SatObsTask(long timestamp, List<SatObservation> observations) {
            this.timestamp = timestamp;
            this.observations = observations;
        }
    }

    private static class GnssSolutionTask {
        final long timestamp;
        final GnssSolution solution;

        GnssSolutionTask(long timestamp, GnssSolution solution) {
            this.timestamp = timestamp;
            this.solution = solution;
        }
    }
}
