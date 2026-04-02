package com.ruoyi.gnss.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;

/**
 * 队列监控告警服务
 *
 * <p>
 * 功能说明：
 * 1. 队列长度阈值告警
 * 2. 队列积压持续时间告警
 * 3. 消费速度低于生产速度预警
 * 4. 告警恢复通知
 * 5. 告警限频，避免刷屏
 * 6. 阈值配置化
 * </p>
 *
 */
@Service
public class QueueMonitorService {

    private static final Logger logger = LoggerFactory.getLogger(QueueMonitorService.class);

    // ==================== 配置参数 ====================

    @Value("${gnss.monitor.enabled:true}")
    private boolean enabled;

    @Value("${gnss.monitor.checkIntervalMs:5000}")
    private long checkIntervalMs;

    @Value("${gnss.monitor.warningThreshold:0.6}")
    private double warningThreshold;

    @Value("${gnss.monitor.criticalThreshold:0.8}")
    private double criticalThreshold;

    @Value("${gnss.monitor.emergencyThreshold:0.95}")
    private double emergencyThreshold;

    @Value("${gnss.monitor.backlogWarningMs:30000}")
    private long backlogWarningMs;

    @Value("${gnss.monitor.backlogCriticalMs:60000}")
    private long backlogCriticalMs;

    @Value("${gnss.monitor.speedCheckWindowMs:10000}")
    private long speedCheckWindowMs;

    @Value("${gnss.monitor.speedRatioWarning:0.5}")
    private double speedRatioWarning;

    @Value("${gnss.monitor.alertCooldownMs:60000}")
    private long alertCooldownMs;

    @Value("${gnss.monitor.recoveryNotifyEnabled:true}")
    private boolean recoveryNotifyEnabled;

    @Value("${gnss.monitor.highUsageThreshold:0.5}")
    private double highUsageThreshold;

    @Value("${gnss.monitor.shutdownTimeoutSec:5}")
    private int shutdownTimeoutSec;

    // ==================== 内部状态 ====================

    /** 队列状态映射 */
    private final ConcurrentHashMap<String, QueueState> queueStates = new ConcurrentHashMap<>();

    /** 告警监听器列表 */
    private final List<Consumer<QueueAlertEvent>> alertListeners = new CopyOnWriteArrayList<>();

    /** 监控调度器 */
    private ScheduledExecutorService scheduler;

    /** 运行状态 */
    private final AtomicBoolean running = new AtomicBoolean(false);

    // ==================== 初始化与销毁 ====================

    @PostConstruct
    public void init() {
        if (!enabled) {
            logger.info("队列监控服务已禁用");
            return;
        }

        scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "Queue-Monitor");
            t.setDaemon(true);
            return t;
        });

        running.set(true);
        scheduler.scheduleAtFixedRate(this::checkAllQueues, checkIntervalMs, checkIntervalMs, TimeUnit.MILLISECONDS);

        logger.info("队列监控服务已启动，检查间隔: {}ms, 告警阈值: warning={}/critical={}/emergency={}",
                checkIntervalMs, (int)(warningThreshold * 100), (int)(criticalThreshold * 100), (int)(emergencyThreshold * 100));
    }

    @PreDestroy
    public void destroy() {
        running.set(false);
        if (scheduler != null) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(shutdownTimeoutSec, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                scheduler.shutdownNow();
            }
        }
        logger.info("队列监控服务已停止");
    }

    // ==================== 队列注册 ====================

    /**
     * 注册队列进行监控
     *
     * @param queueName   队列名称
     * @param queue       队列实例
     * @param capacity    队列容量
     */
    public void registerQueue(String queueName, BlockingQueue<?> queue, int capacity) {
        registerQueue(queueName, queue, capacity, null);
    }

    /**
     * 注册队列进行监控（带站点ID）
     *
     * @param queueName   队列名称
     * @param queue       队列实例
     * @param capacity    队列容量
     * @param stationId   站点ID（可选）
     */
    public void registerQueue(String queueName, BlockingQueue<?> queue, int capacity, String stationId) {
        QueueState state = new QueueState(queueName, queue, capacity, stationId, speedCheckWindowMs, highUsageThreshold);
        queueStates.put(queueName, state);
        logger.info("注册队列监控: {} (容量: {}, 站点: {})", queueName, capacity, stationId != null ? stationId : "全局");
    }

    /**
     * 注销队列监控
     *
     * @param queueName 队列名称
     */
    public void unregisterQueue(String queueName) {
        queueStates.remove(queueName);
        logger.info("注销队列监控: {}", queueName);
    }

    // ==================== 数据更新 ====================

    /**
     * 记录生产事件
     *
     * @param queueName 队列名称
     */
    public void recordProduce(String queueName) {
        QueueState state = queueStates.get(queueName);
        if (state != null) {
            state.recordProduce();
        }
    }

    /**
     * 记录消费事件
     *
     * @param queueName 队列名称
     */
    public void recordConsume(String queueName) {
        QueueState state = queueStates.get(queueName);
        if (state != null) {
            state.recordConsume();
        }
    }

    /**
     * 记录丢弃事件
     *
     * @param queueName 队列名称
     */
    public void recordDrop(String queueName) {
        QueueState state = queueStates.get(queueName);
        if (state != null) {
            state.recordDrop();
        }
    }

    // ==================== 监控检查 ====================

    private void checkAllQueues() {
        if (!running.get()) {
            return;
        }

        for (QueueState state : queueStates.values()) {
            try {
                checkQueue(state);
            } catch (Exception e) {
                logger.error("检查队列 {} 异常: {}", state.queueName, e.getMessage());
            }
        }
    }

    private void checkQueue(QueueState state) {
        int size = state.queue.size();
        int capacity = state.capacity;
        double usageRatio = capacity > 0 ? (double) size / capacity : 0.0;

        // 更新状态
        state.updateSize(size, usageRatio);

        // 计算速率
        long now = System.currentTimeMillis();
        double produceRate = state.getProduceRate(now);
        double consumeRate = state.getConsumeRate(now);

        // 检查告警条件
        QueueAlertEvent.Level previousLevel = state.currentLevel;
        QueueAlertEvent.Level newLevel = determineLevel(usageRatio, state.getBacklogDurationMs(now), produceRate, consumeRate, state.speedCheckWindowMs);

        // 状态变化处理
        if (newLevel != previousLevel) {
            handleLevelChange(state, previousLevel, newLevel, size, capacity, usageRatio, produceRate, consumeRate);
        }

        // 检查是否需要发送周期性告警（告警限频）
        if (newLevel != QueueAlertEvent.Level.INFO && shouldSendAlert(state, now)) {
            sendAlert(state, newLevel, size, capacity, usageRatio, produceRate, consumeRate);
        }
    }

    private QueueAlertEvent.Level determineLevel(double usageRatio, long backlogDurationMs,
                                                 double produceRate, double consumeRate, long speedCheckWindow) {
        // 紧急：队列几乎满或消费停滞
        if (usageRatio >= emergencyThreshold) {
            return QueueAlertEvent.Level.EMERGENCY;
        }

        // 检查消费停滞
        if (produceRate > 0 && consumeRate == 0 && backlogDurationMs > speedCheckWindow) {
            return QueueAlertEvent.Level.EMERGENCY;
        }

        // 严重：队列高使用率或长时间积压
        if (usageRatio >= criticalThreshold || backlogDurationMs >= backlogCriticalMs) {
            return QueueAlertEvent.Level.CRITICAL;
        }

        // 检查消费速度过低
        if (produceRate > 0 && consumeRate > 0 && consumeRate < produceRate * speedRatioWarning) {
            return QueueAlertEvent.Level.WARNING;
        }

        // 警告：队列中等使用率或短时间积压
        if (usageRatio >= warningThreshold || backlogDurationMs >= backlogWarningMs) {
            return QueueAlertEvent.Level.WARNING;
        }

        return QueueAlertEvent.Level.INFO;
    }

    private void handleLevelChange(QueueState state, QueueAlertEvent.Level previousLevel,
                                   QueueAlertEvent.Level newLevel, int size, int capacity,
                                   double usageRatio, double produceRate, double consumeRate) {
        state.currentLevel = newLevel;
        long now = System.currentTimeMillis();

        if (newLevel == QueueAlertEvent.Level.INFO && previousLevel != null && previousLevel != QueueAlertEvent.Level.INFO) {
            // 恢复正常
            if (recoveryNotifyEnabled) {
                sendRecoveryAlert(state, previousLevel, size, capacity, usageRatio);
            }
        } else if (newLevel.ordinal() > previousLevel.ordinal()) {
            // 级别升高，立即发送告警
            sendAlert(state, newLevel, size, capacity, usageRatio, produceRate, consumeRate);
        }
    }

    private boolean shouldSendAlert(QueueState state, long now) {
        // 检查告警冷却时间
        return now - state.lastAlertTime >= alertCooldownMs;
    }

    private void sendAlert(QueueState state, QueueAlertEvent.Level level, int size, int capacity,
                           double usageRatio, double produceRate, double consumeRate) {
        long now = System.currentTimeMillis();
        state.lastAlertTime = now;

        QueueAlertEvent.Type type = determineAlertType(level, usageRatio, produceRate, consumeRate);

        QueueAlertEvent event = QueueAlertEvent.builder()
                .level(level)
                .type(type)
                .queueName(state.queueName)
                .stationId(state.stationId)
                .queueSize(size)
                .queueCapacity(capacity)
                .usageRatio(usageRatio)
                .backlogDurationMs(state.getBacklogDurationMs(now))
                .produceRate(produceRate)
                .consumeRate(consumeRate)
                .build();

        // 记录日志
        logAlert(event);

        // 通知监听器
        notifyListeners(event);
    }

    private void sendRecoveryAlert(QueueState state, QueueAlertEvent.Level previousLevel,
                                   int size, int capacity, double usageRatio) {
        QueueAlertEvent event = QueueAlertEvent.builder()
                .level(QueueAlertEvent.Level.INFO)
                .type(QueueAlertEvent.Type.QUEUE_RECOVERED)
                .queueName(state.queueName)
                .stationId(state.stationId)
                .queueSize(size)
                .queueCapacity(capacity)
                .usageRatio(usageRatio)
                .message(String.format("队列 %s 已恢复正常 (之前级别: %s)", state.queueName, previousLevel))
                .build();

        logAlert(event);
        notifyListeners(event);
    }

    private QueueAlertEvent.Type determineAlertType(QueueAlertEvent.Level level, double usageRatio,
                                                    double produceRate, double consumeRate) {
        if (usageRatio >= emergencyThreshold) {
            return QueueAlertEvent.Type.QUEUE_FULL;
        }
        if (produceRate > 0 && consumeRate == 0) {
            return QueueAlertEvent.Type.CONSUME_STALLED;
        }
        if (produceRate > 0 && consumeRate > 0 && consumeRate < produceRate * speedRatioWarning) {
            return QueueAlertEvent.Type.CONSUME_SPEED_LOW;
        }
        if (usageRatio >= warningThreshold) {
            return QueueAlertEvent.Type.QUEUE_LENGTH_THRESHOLD;
        }
        return QueueAlertEvent.Type.QUEUE_BACKLOG_DURATION;
    }

    private void logAlert(QueueAlertEvent event) {
        switch (event.getLevel()) {
            case EMERGENCY:
                logger.error("【队列告警】{}", event.getMessage());
                break;
            case CRITICAL:
                logger.error("【队列告警】{}", event.getMessage());
                break;
            case WARNING:
                logger.warn("【队列告警】{}", event.getMessage());
                break;
            case INFO:
                logger.info("【队列恢复】{}", event.getMessage());
                break;
        }
    }

    private void notifyListeners(QueueAlertEvent event) {
        for (Consumer<QueueAlertEvent> listener : alertListeners) {
            try {
                listener.accept(event);
            } catch (Exception e) {
                logger.error("告警监听器异常: {}", e.getMessage());
            }
        }
    }

    // ==================== 监听器管理 ====================

    /**
     * 添加告警监听器
     *
     * @param listener 监听器
     */
    public void addAlertListener(Consumer<QueueAlertEvent> listener) {
        alertListeners.add(listener);
    }

    /**
     * 移除告警监听器
     *
     * @param listener 监听器
     */
    public void removeAlertListener(Consumer<QueueAlertEvent> listener) {
        alertListeners.remove(listener);
    }

    // ==================== 动态阈值调整 ====================

    /**
     * 更新告警阈值（运行时动态调整）
     *
     * @param warning    警告阈值 (0.0-1.0)
     * @param critical   严重阈值 (0.0-1.0)
     * @param emergency  紧急阈值 (0.0-1.0)
     */
    public void updateThresholds(double warning, double critical, double emergency) {
        if (warning < 0 || warning > 1 || critical < 0 || critical > 1 || emergency < 0 || emergency > 1) {
            throw new IllegalArgumentException("阈值必须在 0.0-1.0 范围内");
        }
        if (warning >= critical || critical >= emergency) {
            throw new IllegalArgumentException("阈值必须满足: warning < critical < emergency");
        }

        this.warningThreshold = warning;
        this.criticalThreshold = critical;
        this.emergencyThreshold = emergency;

        logger.info("告警阈值已更新: warning={}/critical={}/emergency={}",
                (int)(warning * 100), (int)(critical * 100), (int)(emergency * 100));
    }

    /**
     * 更新高使用率阈值（运行时动态调整）
     *
     * @param threshold 高使用率阈值 (0.0-1.0)
     */
    public void updateHighUsageThreshold(double threshold) {
        if (threshold < 0 || threshold > 1) {
            throw new IllegalArgumentException("阈值必须在 0.0-1.0 范围内");
        }

        this.highUsageThreshold = threshold;

        // 更新所有已注册队列的阈值
        for (QueueState state : queueStates.values()) {
            state.highUsageThreshold = threshold;
        }

        logger.info("高使用率阈值已更新: {}", (int)(threshold * 100));
    }

    /**
     * 更新告警冷却时间
     *
     * @param cooldownMs 冷却时间（毫秒）
     */
    public void updateAlertCooldown(long cooldownMs) {
        if (cooldownMs < 0) {
            throw new IllegalArgumentException("冷却时间不能为负数");
        }

        this.alertCooldownMs = cooldownMs;
        logger.info("告警冷却时间已更新: {}ms", cooldownMs);
    }

    /**
     * 获取当前阈值配置
     *
     * @return 阈值配置信息
     */
    public Map<String, Object> getThresholdConfig() {
        Map<String, Object> config = new LinkedHashMap<>();
        config.put("warningThreshold", warningThreshold);
        config.put("criticalThreshold", criticalThreshold);
        config.put("emergencyThreshold", emergencyThreshold);
        config.put("highUsageThreshold", highUsageThreshold);
        config.put("backlogWarningMs", backlogWarningMs);
        config.put("backlogCriticalMs", backlogCriticalMs);
        config.put("speedRatioWarning", speedRatioWarning);
        config.put("alertCooldownMs", alertCooldownMs);
        config.put("checkIntervalMs", checkIntervalMs);
        return config;
    }

    // ==================== 统计信息 ====================

    /**
     * 获取队列状态摘要
     *
     * @return 状态摘要字符串
     */
    public String getStatusSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("QueueMonitor[enabled=").append(enabled);

        if (!queueStates.isEmpty()) {
            sb.append(", queues={");
            boolean first = true;
            for (Map.Entry<String, QueueState> entry : queueStates.entrySet()) {
                if (!first) sb.append(", ");
                QueueState state = entry.getValue();
                sb.append(entry.getKey())
                        .append(": ")
                        .append(state.currentSize)
                        .append("/")
                        .append(state.capacity)
                        .append("(")
                        .append(String.format("%.0f%%", state.currentUsageRatio * 100))
                        .append(")");
                first = false;
            }
            sb.append("}");
        }

        sb.append("]");
        return sb.toString();
    }

    /**
     * 获取所有队列的详细状态
     *
     * @return 队列状态列表
     */
    public List<Map<String, Object>> getQueueDetails() {
        List<Map<String, Object>> result = new ArrayList<>();
        long now = System.currentTimeMillis();

        for (QueueState state : queueStates.values()) {
            Map<String, Object> details = new LinkedHashMap<>();
            details.put("queueName", state.queueName);
            details.put("stationId", state.stationId);
            details.put("size", state.currentSize);
            details.put("capacity", state.capacity);
            details.put("usageRatio", state.currentUsageRatio);
            details.put("level", state.currentLevel.name());
            details.put("produceRate", state.getProduceRate(now));
            details.put("consumeRate", state.getConsumeRate(now));
            details.put("backlogDurationMs", state.getBacklogDurationMs(now));
            details.put("totalProduced", state.totalProduced.get());
            details.put("totalConsumed", state.totalConsumed.get());
            details.put("totalDropped", state.totalDropped.get());
            result.add(details);
        }

        return result;
    }

    // ==================== 内部状态类 ====================

    /**
     * 队列状态内部类
     *
     * 修复说明：将 speedCheckWindowMs 和 highUsageThreshold 作为构造函数参数传入，
     * 解决静态内部类无法访问外部类实例字段的问题
     */
    private static class QueueState {
        final String queueName;
        final BlockingQueue<?> queue;
        final int capacity;
        final String stationId;
        final long speedCheckWindowMs;
        volatile double highUsageThreshold;

        volatile int currentSize;
        volatile double currentUsageRatio;
        volatile QueueAlertEvent.Level currentLevel = QueueAlertEvent.Level.INFO;

        volatile long highUsageStartTime = 0;
        volatile long lastAlertTime = 0;

        final AtomicLong totalProduced = new AtomicLong(0);
        final AtomicLong totalConsumed = new AtomicLong(0);
        final AtomicLong totalDropped = new AtomicLong(0);

        final ConcurrentLinkedDeque<long[]> produceHistory = new ConcurrentLinkedDeque<>();
        final ConcurrentLinkedDeque<long[]> consumeHistory = new ConcurrentLinkedDeque<>();

        QueueState(String queueName, BlockingQueue<?> queue, int capacity, String stationId,
                   long speedCheckWindowMs, double highUsageThreshold) {
            this.queueName = queueName;
            this.queue = queue;
            this.capacity = capacity;
            this.stationId = stationId;
            this.speedCheckWindowMs = speedCheckWindowMs;
            this.highUsageThreshold = highUsageThreshold;
        }

        void updateSize(int size, double usageRatio) {
            this.currentSize = size;
            this.currentUsageRatio = usageRatio;

            if (usageRatio >= highUsageThreshold && highUsageStartTime == 0) {
                highUsageStartTime = System.currentTimeMillis();
            } else if (usageRatio < highUsageThreshold) {
                highUsageStartTime = 0;
            }
        }

        void recordProduce() {
            totalProduced.incrementAndGet();
            long now = System.currentTimeMillis();
            produceHistory.addLast(new long[]{now, 1});
            cleanupHistory(produceHistory, now);
        }

        void recordConsume() {
            totalConsumed.incrementAndGet();
            long now = System.currentTimeMillis();
            consumeHistory.addLast(new long[]{now, 1});
            cleanupHistory(consumeHistory, now);
        }

        void recordDrop() {
            totalDropped.incrementAndGet();
        }

        void cleanupHistory(ConcurrentLinkedDeque<long[]> history, long now) {
            long cutoff = now - speedCheckWindowMs * 2;
            while (!history.isEmpty() && history.peekFirst()[0] < cutoff) {
                history.pollFirst();
            }
        }

        double getProduceRate(long now) {
            return calculateRate(produceHistory, now);
        }

        double getConsumeRate(long now) {
            return calculateRate(consumeHistory, now);
        }

        private double calculateRate(ConcurrentLinkedDeque<long[]> history, long now) {
            long cutoff = now - speedCheckWindowMs;
            int count = 0;
            for (long[] entry : history) {
                if (entry[0] >= cutoff) {
                    count++;
                }
            }
            return count * 1000.0 / speedCheckWindowMs;
        }

        long getBacklogDurationMs(long now) {
            if (highUsageStartTime == 0 || currentUsageRatio < highUsageThreshold) {
                return 0;
            }
            return now - highUsageStartTime;
        }
    }
}
