package com.ruoyi.gnss.service;

import java.time.Instant;

/**
 * 队列告警事件
 *
 * @author GNSS Team
 * @date 2026-03-26
 */
public class QueueAlertEvent {

    /**
     * 告警级别
     */
    public enum Level {
        /** 信息：正常状态恢复 */
        INFO(0),
        /** 警告：队列积压 */
        WARNING(1),
        /** 严重：队列即将满 */
        CRITICAL(2),
        /** 紧急：队列已满或消费停滞 */
        EMERGENCY(3);

        private final int severity;

        Level(int severity) {
            this.severity = severity;
        }

        public int getSeverity() {
            return severity;
        }
    }

    /**
     * 告警类型
     */
    public enum Type {
        /** 队列长度超过阈值 */
        QUEUE_LENGTH_THRESHOLD,
        /** 队列积压持续时间过长 */
        QUEUE_BACKLOG_DURATION,
        /** 消费速度低于生产速度 */
        CONSUME_SPEED_LOW,
        /** 队列已满 */
        QUEUE_FULL,
        /** 队列恢复正常 */
        QUEUE_RECOVERED,
        /** 消费停滞 */
        CONSUME_STALLED
    }

    /** 告警ID */
    private final String alertId;

    /** 告警级别 */
    private final Level level;

    /** 告警类型 */
    private final Type type;

    /** 队列名称 */
    private final String queueName;

    /** 站点ID（可选） */
    private final String stationId;

    /** 当前队列长度 */
    private final int queueSize;

    /** 队列容量 */
    private final int queueCapacity;

    /** 使用率 (0.0 - 1.0) */
    private final double usageRatio;

    /** 积压持续时间（毫秒） */
    private final long backlogDurationMs;

    /** 生产速率（条/秒） */
    private final double produceRate;

    /** 消费速率（条/秒） */
    private final double consumeRate;

    /** 告警时间 */
    private final Instant timestamp;

    /** 告警消息 */
    private final String message;

    private QueueAlertEvent(Builder builder) {
        this.alertId = builder.alertId;
        this.level = builder.level;
        this.type = builder.type;
        this.queueName = builder.queueName;
        this.stationId = builder.stationId;
        this.queueSize = builder.queueSize;
        this.queueCapacity = builder.queueCapacity;
        this.usageRatio = builder.usageRatio;
        this.backlogDurationMs = builder.backlogDurationMs;
        this.produceRate = builder.produceRate;
        this.consumeRate = builder.consumeRate;
        this.timestamp = builder.timestamp != null ? builder.timestamp : Instant.now();
        this.message = builder.message;
    }

    // ==================== Getters ====================

    public String getAlertId() {
        return alertId;
    }

    public Level getLevel() {
        return level;
    }

    public Type getType() {
        return type;
    }

    public String getQueueName() {
        return queueName;
    }

    public String getStationId() {
        return stationId;
    }

    public int getQueueSize() {
        return queueSize;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public double getUsageRatio() {
        return usageRatio;
    }

    public long getBacklogDurationMs() {
        return backlogDurationMs;
    }

    public double getProduceRate() {
        return produceRate;
    }

    public double getConsumeRate() {
        return consumeRate;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public String getMessage() {
        return message;
    }

    // ==================== Builder ====================

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private String alertId;
        private Level level;
        private Type type;
        private String queueName;
        private String stationId;
        private int queueSize;
        private int queueCapacity;
        private double usageRatio;
        private long backlogDurationMs;
        private double produceRate;
        private double consumeRate;
        private Instant timestamp;
        private String message;

        public Builder alertId(String alertId) {
            this.alertId = alertId;
            return this;
        }

        public Builder level(Level level) {
            this.level = level;
            return this;
        }

        public Builder type(Type type) {
            this.type = type;
            return this;
        }

        public Builder queueName(String queueName) {
            this.queueName = queueName;
            return this;
        }

        public Builder stationId(String stationId) {
            this.stationId = stationId;
            return this;
        }

        public Builder queueSize(int queueSize) {
            this.queueSize = queueSize;
            return this;
        }

        public Builder queueCapacity(int queueCapacity) {
            this.queueCapacity = queueCapacity;
            return this;
        }

        public Builder usageRatio(double usageRatio) {
            this.usageRatio = usageRatio;
            return this;
        }

        public Builder backlogDurationMs(long backlogDurationMs) {
            this.backlogDurationMs = backlogDurationMs;
            return this;
        }

        public Builder produceRate(double produceRate) {
            this.produceRate = produceRate;
            return this;
        }

        public Builder consumeRate(double consumeRate) {
            this.consumeRate = consumeRate;
            return this;
        }

        public Builder timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }

        public Builder message(String message) {
            this.message = message;
            return this;
        }

        public QueueAlertEvent build() {
            if (alertId == null) {
                alertId = java.util.UUID.randomUUID().toString().substring(0, 8);
            }
            if (timestamp == null) {
                timestamp = Instant.now();
            }
            if (message == null) {
                message = buildDefaultMessage();
            }
            return new QueueAlertEvent(this);
        }

        private String buildDefaultMessage() {
            StringBuilder sb = new StringBuilder();
            sb.append("[").append(level).append("] ");
            sb.append("队列 ").append(queueName);

            switch (type) {
                case QUEUE_LENGTH_THRESHOLD:
                    sb.append(" 长度超过阈值: ").append(queueSize).append("/").append(queueCapacity);
                    sb.append(String.format(" (%.1f%%)", usageRatio * 100));
                    break;
                case QUEUE_BACKLOG_DURATION:
                    sb.append(" 积压持续: ").append(backlogDurationMs / 1000).append("秒");
                    break;
                case CONSUME_SPEED_LOW:
                    sb.append(" 消费速度低于生产速度");
                    sb.append(String.format(" (生产: %.1f/s, 消费: %.1f/s)", produceRate, consumeRate));
                    break;
                case QUEUE_FULL:
                    sb.append(" 已满!");
                    break;
                case QUEUE_RECOVERED:
                    sb.append(" 已恢复正常");
                    break;
                case CONSUME_STALLED:
                    sb.append(" 消费停滞!");
                    break;
            }

            if (stationId != null) {
                sb.append(" [站点: ").append(stationId).append("]");
            }

            return sb.toString();
        }
    }

    @Override
    public String toString() {
        return String.format("QueueAlertEvent[%s, %s, %s, queue=%s, size=%d/%d, ratio=%.1f%%]",
                alertId, level, type, queueName, queueSize, queueCapacity, usageRatio * 100);
    }
}
