package com.ruoyi.gnss.recorder;

import org.springframework.boot.context.properties.ConfigurationProperties;

/**
 * GNSS 录制服务配置属性类
 * 功能说明:
 * - 从 application.yml 中读取 gnss.recorder 配置项
 * - 提供默认值和参数校验
 * - 支持动态刷新配置
 */
@ConfigurationProperties(prefix = "gnss.recorder")
public class GnssRecorderProperties {

    // ==================== 基础配置 ====================

    /**
     * 是否启用录制服务
     */
    private boolean enabled = false;

    /**
     * 数据保存根目录
     */
    private String saveDir = "./gnss-data";

    /**
     * 站点ID（用于创建子目录）
     */
    private String stationId = "default";

    // ==================== 文件配置 ====================

    /**
     * 文件前缀
     */
    private String filePrefix = "";

    /**
     * 文件后缀
     */
    private String fileSuffix = ".bin";

    /**
     * 是否按日期创建子目录
     */
    private boolean dateSubdirectory = true;

    /**
     * 日期目录格式
     */
    private String dateFormat = "yyyy-MM-dd";

    /**
     * 时区（默认系统时区）
     */
    private String timezone = "Asia/Shanghai";

    // ==================== 写入配置 ====================

    /**
     * 缓冲区大小（字节）
     */
    private int bufferSize = 65536;

    /**
     * 刷新间隔（毫秒）
     */
    private long flushIntervalMs = 3000;

    /**
     * 最大刷新间隔（毫秒）- 即使数据量小也强制刷新
     */
    private long maxFlushIntervalMs = 10000;

    /**
     * 写入队列容量
     */
    private int queueCapacity = 10000;

    // ==================== 跨天切换配置 ====================

    /**
     * 跨天切换检查间隔（毫秒）
     */
    private long daySwitchCheckIntervalMs = 1000;

    /**
     * 跨天切换缓冲时间（毫秒）- 提前多少毫秒开始准备切换
     */
    private long daySwitchBufferMs = 100;

    // ==================== 数据中断检测配置 ====================

    /**
     * 数据中断检测阈值（毫秒）- 超过此时间无数据视为中断
     */
    private long interruptionThresholdMs = 5000;

    /**
     * 数据中断告警阈值（毫秒）- 超过此时间记录告警日志
     */
    private long interruptionWarningMs = 30000;

    /**
     * 数据中断严重告警阈值（毫秒）
     */
    private long interruptionCriticalMs = 60000;

    /**
     * 无数据检测间隔（毫秒）
     */
    private long noDataCheckIntervalMs = 1000;

    // ==================== 健康检查配置 ====================

    /**
     * 健康检查间隔（毫秒）
     */
    private long healthCheckIntervalMs = 5000;

    /**
     * 是否记录详细健康日志
     */
    private boolean detailedHealthLog = false;

    // ==================== 异常恢复配置 ====================

    /**
     * 文件写入失败重试次数
     */
    private int writeRetryCount = 3;

    /**
     * 文件写入失败重试间隔（毫秒）
     */
    private long writeRetryDelayMs = 1000;

    /**
     * 磁盘空间检查间隔（毫秒）
     */
    private long diskSpaceCheckIntervalMs = 60000;

    /**
     * 最小磁盘空间（MB）- 低于此值停止录制
     */
    private long minDiskSpaceMb = 100;

    // ==================== 录制内容配置 ====================

    /**
     * 是否录制 RTCM 数据
     */
    private boolean recordRtcm = true;

    /**
     * 是否录制 NMEA 数据
     */
    private boolean recordNmea = true;

    /**
     * 是否在文件中记录中断标记
     */
    private boolean markInterruption = true;

    // ==================== 性能配置 ====================

    /**
     * 写入线程名称
     */
    private String writerThreadName = "GNSS-Recorder-Writer";

    /**
     * 监控线程名称
     */
    private String monitorThreadName = "GNSS-Recorder-Monitor";

    // ==================== Getter 和 Setter 方法 ====================

    public boolean isEnabled() {
        return enabled;
    }

    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    public String getSaveDir() {
        return saveDir;
    }

    public void setSaveDir(String saveDir) {
        this.saveDir = saveDir;
    }

    public String getStationId() {
        return stationId;
    }

    public void setStationId(String stationId) {
        this.stationId = stationId;
    }

    public String getFilePrefix() {
        return filePrefix;
    }

    public void setFilePrefix(String filePrefix) {
        this.filePrefix = filePrefix;
    }

    public String getFileSuffix() {
        return fileSuffix;
    }

    public void setFileSuffix(String fileSuffix) {
        this.fileSuffix = fileSuffix;
    }

    public boolean isDateSubdirectory() {
        return dateSubdirectory;
    }

    public void setDateSubdirectory(boolean dateSubdirectory) {
        this.dateSubdirectory = dateSubdirectory;
    }

    public String getDateFormat() {
        return dateFormat;
    }

    public void setDateFormat(String dateFormat) {
        this.dateFormat = dateFormat;
    }

    public String getTimezone() {
        return timezone;
    }

    public void setTimezone(String timezone) {
        this.timezone = timezone;
    }

    public int getBufferSize() {
        return bufferSize;
    }

    public void setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
    }

    public long getFlushIntervalMs() {
        return flushIntervalMs;
    }

    public void setFlushIntervalMs(long flushIntervalMs) {
        this.flushIntervalMs = flushIntervalMs;
    }

    public long getMaxFlushIntervalMs() {
        return maxFlushIntervalMs;
    }

    public void setMaxFlushIntervalMs(long maxFlushIntervalMs) {
        this.maxFlushIntervalMs = maxFlushIntervalMs;
    }

    public int getQueueCapacity() {
        return queueCapacity;
    }

    public void setQueueCapacity(int queueCapacity) {
        this.queueCapacity = queueCapacity;
    }

    public long getDaySwitchCheckIntervalMs() {
        return daySwitchCheckIntervalMs;
    }

    public void setDaySwitchCheckIntervalMs(long daySwitchCheckIntervalMs) {
        this.daySwitchCheckIntervalMs = daySwitchCheckIntervalMs;
    }

    public long getDaySwitchBufferMs() {
        return daySwitchBufferMs;
    }

    public void setDaySwitchBufferMs(long daySwitchBufferMs) {
        this.daySwitchBufferMs = daySwitchBufferMs;
    }

    public long getInterruptionThresholdMs() {
        return interruptionThresholdMs;
    }

    public void setInterruptionThresholdMs(long interruptionThresholdMs) {
        this.interruptionThresholdMs = interruptionThresholdMs;
    }

    public long getInterruptionWarningMs() {
        return interruptionWarningMs;
    }

    public void setInterruptionWarningMs(long interruptionWarningMs) {
        this.interruptionWarningMs = interruptionWarningMs;
    }

    public long getInterruptionCriticalMs() {
        return interruptionCriticalMs;
    }

    public void setInterruptionCriticalMs(long interruptionCriticalMs) {
        this.interruptionCriticalMs = interruptionCriticalMs;
    }

    public long getNoDataCheckIntervalMs() {
        return noDataCheckIntervalMs;
    }

    public void setNoDataCheckIntervalMs(long noDataCheckIntervalMs) {
        this.noDataCheckIntervalMs = noDataCheckIntervalMs;
    }

    public long getHealthCheckIntervalMs() {
        return healthCheckIntervalMs;
    }

    public void setHealthCheckIntervalMs(long healthCheckIntervalMs) {
        this.healthCheckIntervalMs = healthCheckIntervalMs;
    }

    public boolean isDetailedHealthLog() {
        return detailedHealthLog;
    }

    public void setDetailedHealthLog(boolean detailedHealthLog) {
        this.detailedHealthLog = detailedHealthLog;
    }

    public int getWriteRetryCount() {
        return writeRetryCount;
    }

    public void setWriteRetryCount(int writeRetryCount) {
        this.writeRetryCount = writeRetryCount;
    }

    public long getWriteRetryDelayMs() {
        return writeRetryDelayMs;
    }

    public void setWriteRetryDelayMs(long writeRetryDelayMs) {
        this.writeRetryDelayMs = writeRetryDelayMs;
    }

    public long getDiskSpaceCheckIntervalMs() {
        return diskSpaceCheckIntervalMs;
    }

    public void setDiskSpaceCheckIntervalMs(long diskSpaceCheckIntervalMs) {
        this.diskSpaceCheckIntervalMs = diskSpaceCheckIntervalMs;
    }

    public long getMinDiskSpaceMb() {
        return minDiskSpaceMb;
    }

    public void setMinDiskSpaceMb(long minDiskSpaceMb) {
        this.minDiskSpaceMb = minDiskSpaceMb;
    }

    public boolean isRecordRtcm() {
        return recordRtcm;
    }

    public void setRecordRtcm(boolean recordRtcm) {
        this.recordRtcm = recordRtcm;
    }

    public boolean isRecordNmea() {
        return recordNmea;
    }

    public void setRecordNmea(boolean recordNmea) {
        this.recordNmea = recordNmea;
    }

    public boolean isMarkInterruption() {
        return markInterruption;
    }

    public void setMarkInterruption(boolean markInterruption) {
        this.markInterruption = markInterruption;
    }

    public String getWriterThreadName() {
        return writerThreadName;
    }

    public void setWriterThreadName(String writerThreadName) {
        this.writerThreadName = writerThreadName;
    }

    public String getMonitorThreadName() {
        return monitorThreadName;
    }

    public void setMonitorThreadName(String monitorThreadName) {
        this.monitorThreadName = monitorThreadName;
    }
}
