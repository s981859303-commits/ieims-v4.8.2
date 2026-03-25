package com.ruoyi.gnss.recorder;

import java.io.File;
import java.util.Date;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * GNSS 录制服务健康状态类
 * 功能说明:
 * - 记录录制服务的运行状态
 * - 统计数据接收和写入情况
 * - 跟踪中断和恢复事件
 * - 提供健康检查接口
 */
public class RecorderHealthStatus {

    // ==================== 服务状态 ====================

    /**
     * 服务是否正在运行
     */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * 服务是否健康
     */
    private final AtomicBoolean healthy = new AtomicBoolean(true);

    /**
     * 服务启动时间
     */
    private volatile long startTime = 0;

    /**
     * 服务停止时间
     */
    private volatile long stopTime = 0;

    /**
     * 最后一次数据接收时间
     */
    private final AtomicLong lastDataReceiveTime = new AtomicLong(0);

    /**
     * 最后一次数据写入时间
     */
    private final AtomicLong lastDataWriteTime = new AtomicLong(0);

    // ==================== 统计数据 ====================

    /**
     * 接收到的 RTCM 数据包数量
     */
    private final AtomicLong rtcmPacketCount = new AtomicLong(0);

    /**
     * 接收到的 NMEA 数据包数量
     */
    private final AtomicLong nmeaPacketCount = new AtomicLong(0);

    /**
     * 接收到的总字节数
     */
    private final AtomicLong totalBytesReceived = new AtomicLong(0);

    /**
     * 写入的总字节数
     */
    private final AtomicLong totalBytesWritten = new AtomicLong(0);

    /**
     * 写入失败次数
     */
    private final AtomicLong writeFailureCount = new AtomicLong(0);

    /**
     * 文件切换次数
     */
    private final AtomicLong fileSwitchCount = new AtomicLong(0);

    /**
     * 跨天切换次数
     */
    private final AtomicLong daySwitchCount = new AtomicLong(0);

    // ==================== 中断状态 ====================

    /**
     * 当前是否处于数据中断状态
     */
    private final AtomicBoolean interrupted = new AtomicBoolean(false);

    /**
     * 中断开始时间
     */
    private volatile long interruptionStartTime = 0;

    /**
     * 累计中断次数
     */
    private final AtomicLong interruptionCount = new AtomicLong(0);

    /**
     * 累计中断时长（毫秒）
     */
    private final AtomicLong totalInterruptionTime = new AtomicLong(0);

    /**
     * 最长单次中断时长（毫秒）
     */
    private final AtomicLong maxInterruptionTime = new AtomicLong(0);

    /**
     * 最后一次中断原因
     */
    private volatile String lastInterruptionReason = null;

    // ==================== 文件状态 ====================

    /**
     * 当前文件路径
     */
    private volatile String currentFilePath = null;

    /**
     * 当前文件大小
     */
    private final AtomicLong currentFileSize = new AtomicLong(0);

    /**
     * 当前文件日期（格式：yyyy-MM-dd）
     */
    private volatile String currentFileDate = null;

    // ==================== 错误状态 ====================

    /**
     * 最后一次错误信息
     */
    private volatile String lastError = null;

    /**
     * 最后一次错误时间
     */
    private volatile long lastErrorTime = 0;

    /**
     * 磁盘空间不足标志
     */
    private final AtomicBoolean diskSpaceLow = new AtomicBoolean(false);

    // ==================== 状态更新方法 ====================

    /**
     * 标记服务启动
     */
    public void markStarted() {
        this.startTime = System.currentTimeMillis();
        this.running.set(true);
        this.healthy.set(true);
    }

    /**
     * 标记服务停止
     */
    public void markStopped() {
        this.stopTime = System.currentTimeMillis();
        this.running.set(false);
    }

    /**
     * 记录数据接收
     */
    public void recordDataReceived(int bytes, boolean isRtcm) {
        this.lastDataReceiveTime.set(System.currentTimeMillis());
        this.totalBytesReceived.addAndGet(bytes);
        if (isRtcm) {
            this.rtcmPacketCount.incrementAndGet();
        } else {
            this.nmeaPacketCount.incrementAndGet();
        }
    }

    /**
     * 记录数据写入
     */
    public void recordDataWritten(int bytes) {
        this.lastDataWriteTime.set(System.currentTimeMillis());
        this.totalBytesWritten.addAndGet(bytes);
        this.currentFileSize.addAndGet(bytes);
    }

    /**
     * 记录写入失败
     */
    public void recordWriteFailure() {
        this.writeFailureCount.incrementAndGet();
    }

    /**
     * 记录文件切换
     */
    public void recordFileSwitch(String newFilePath, String newFileDate) {
        this.fileSwitchCount.incrementAndGet();
        this.currentFilePath = newFilePath;
        this.currentFileDate = newFileDate;
        this.currentFileSize.set(0);
    }

    /**
     * 记录跨天切换
     */
    public void recordDaySwitch() {
        this.daySwitchCount.incrementAndGet();
    }

    /**
     * 开始中断
     */
    public void startInterruption(String reason) {
        if (!interrupted.getAndSet(true)) {
            this.interruptionStartTime = System.currentTimeMillis();
            this.interruptionCount.incrementAndGet();
            this.lastInterruptionReason = reason;
        }
    }

    /**
     * 结束中断
     */
    public void endInterruption() {
        if (interrupted.getAndSet(false)) {
            long duration = System.currentTimeMillis() - interruptionStartTime;
            this.totalInterruptionTime.addAndGet(duration);
            if (duration > maxInterruptionTime.get()) {
                this.maxInterruptionTime.set(duration);
            }
        }
    }

    /**
     * 获取当前中断时长
     */
    public long getCurrentInterruptionDuration() {
        if (interrupted.get()) {
            return System.currentTimeMillis() - interruptionStartTime;
        }
        return 0;
    }

    /**
     * 记录错误
     */
    public void recordError(String error) {
        this.lastError = error;
        this.lastErrorTime = System.currentTimeMillis();
    }

    /**
     * 设置磁盘空间状态
     */
    public void setDiskSpaceLow(boolean low) {
        this.diskSpaceLow.set(low);
        if (low) {
            this.healthy.set(false);
        }
    }

    /**
     * 设置健康状态
     */
    public void setHealthy(boolean healthy) {
        this.healthy.set(healthy);
    }

    // ==================== 状态查询方法 ====================

    public boolean isRunning() {
        return running.get();
    }

    public boolean isHealthy() {
        return healthy.get();
    }

    public boolean isInterrupted() {
        return interrupted.get();
    }

    public long getStartTime() {
        return startTime;
    }

    public long getStopTime() {
        return stopTime;
    }

    public long getLastDataReceiveTime() {
        return lastDataReceiveTime.get();
    }

    public long getLastDataWriteTime() {
        return lastDataWriteTime.get();
    }

    public long getRtcmPacketCount() {
        return rtcmPacketCount.get();
    }

    public long getNmeaPacketCount() {
        return nmeaPacketCount.get();
    }

    public long getTotalBytesReceived() {
        return totalBytesReceived.get();
    }

    public long getTotalBytesWritten() {
        return totalBytesWritten.get();
    }

    public long getWriteFailureCount() {
        return writeFailureCount.get();
    }

    public long getFileSwitchCount() {
        return fileSwitchCount.get();
    }

    public long getDaySwitchCount() {
        return daySwitchCount.get();
    }

    public long getInterruptionCount() {
        return interruptionCount.get();
    }

    public long getTotalInterruptionTime() {
        return totalInterruptionTime.get();
    }

    public long getMaxInterruptionTime() {
        return maxInterruptionTime.get();
    }

    public String getLastInterruptionReason() {
        return lastInterruptionReason;
    }

    public String getCurrentFilePath() {
        return currentFilePath;
    }

    public long getCurrentFileSize() {
        return currentFileSize.get();
    }

    public String getCurrentFileDate() {
        return currentFileDate;
    }

    public String getLastError() {
        return lastError;
    }

    public long getLastErrorTime() {
        return lastErrorTime;
    }

    public boolean isDiskSpaceLow() {
        return diskSpaceLow.get();
    }

    /**
     * 获取运行时长（毫秒）
     */
    public long getRunningDuration() {
        if (startTime == 0) {
            return 0;
        }
        long endTime = running.get() ? System.currentTimeMillis() : stopTime;
        return endTime - startTime;
    }

    /**
     * 获取距离上次接收数据的时间（毫秒）
     */
    public long getTimeSinceLastData() {
        long last = lastDataReceiveTime.get();
        if (last == 0) {
            return 0;
        }
        return System.currentTimeMillis() - last;
    }

    /**
     * 重置统计数据
     */
    public void resetStatistics() {
        rtcmPacketCount.set(0);
        nmeaPacketCount.set(0);
        totalBytesReceived.set(0);
        totalBytesWritten.set(0);
        writeFailureCount.set(0);
        fileSwitchCount.set(0);
        daySwitchCount.set(0);
        interruptionCount.set(0);
        totalInterruptionTime.set(0);
        maxInterruptionTime.set(0);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("RecorderHealthStatus {\n");
        sb.append("  running: ").append(running.get()).append(",\n");
        sb.append("  healthy: ").append(healthy.get()).append(",\n");
        sb.append("  interrupted: ").append(interrupted.get()).append(",\n");
        sb.append("  runningDuration: ").append(formatDuration(getRunningDuration())).append(",\n");
        sb.append("  timeSinceLastData: ").append(formatDuration(getTimeSinceLastData())).append(",\n");
        sb.append("  rtcmPackets: ").append(rtcmPacketCount.get()).append(",\n");
        sb.append("  nmeaPackets: ").append(nmeaPacketCount.get()).append(",\n");
        sb.append("  totalBytesReceived: ").append(formatBytes(totalBytesReceived.get())).append(",\n");
        sb.append("  totalBytesWritten: ").append(formatBytes(totalBytesWritten.get())).append(",\n");
        sb.append("  writeFailures: ").append(writeFailureCount.get()).append(",\n");
        sb.append("  fileSwitches: ").append(fileSwitchCount.get()).append(",\n");
        sb.append("  daySwitches: ").append(daySwitchCount.get()).append(",\n");
        sb.append("  interruptions: ").append(interruptionCount.get()).append(",\n");
        sb.append("  totalInterruptionTime: ").append(formatDuration(totalInterruptionTime.get())).append(",\n");
        sb.append("  currentFile: ").append(currentFilePath).append(",\n");
        sb.append("  currentFileSize: ").append(formatBytes(currentFileSize.get())).append(",\n");
        sb.append("  lastError: ").append(lastError).append("\n");
        sb.append("}");
        return sb.toString();
    }

    private String formatDuration(long ms) {
        if (ms < 1000) {
            return ms + "ms";
        } else if (ms < 60000) {
            return (ms / 1000) + "s";
        } else if (ms < 3600000) {
            return (ms / 60000) + "m " + ((ms % 60000) / 1000) + "s";
        } else {
            return (ms / 3600000) + "h " + ((ms % 3600000) / 60000) + "m";
        }
    }

    private String formatBytes(long bytes) {
        if (bytes < 1024) {
            return bytes + "B";
        } else if (bytes < 1024 * 1024) {
            return String.format("%.2fKB", bytes / 1024.0);
        } else if (bytes < 1024 * 1024 * 1024) {
            return String.format("%.2fMB", bytes / (1024.0 * 1024));
        } else {
            return String.format("%.2fGB", bytes / (1024.0 * 1024 * 1024));
        }
    }
}
