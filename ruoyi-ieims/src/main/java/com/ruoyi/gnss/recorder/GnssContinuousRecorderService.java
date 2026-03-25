package com.ruoyi.gnss.recorder;

import com.ruoyi.gnss.service.GnssDataListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.TimeZone;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.ReentrantLock;

/**
 * GNSS 数据 24 小时不间断录制服务
 * 功能特性:
 * 1. 7x24 小时不间断运行，支持服务重启后自动恢复
 * 2. 自动处理卫星闪烁、数据中断、设备断连等异常情况
 * 3. 跨天自动切换文件，确保数据不丢失
 * 4. 线程安全的数据写入，支持高并发场景
 * 5. 完善的健康检查和监控机制
 * 6. 磁盘空间检测和自动保护
 */
@Service
@ConditionalOnProperty(name = "gnss.recorder.enabled", havingValue = "true")
@ConditionalOnBean(GnssRecorderProperties.class)
public class GnssContinuousRecorderService implements GnssDataListener {

    private static final Logger logger = LoggerFactory.getLogger(GnssContinuousRecorderService.class);

    // ==================== 依赖注入 ====================

    /**
     * 配置属性 - 使用 setter 注入，避免构造函数注入问题
     */
    private GnssRecorderProperties properties;

    // ==================== 核心组件 ====================

    /**
     * 健康状态
     */
    private final RecorderHealthStatus healthStatus = new RecorderHealthStatus();

    /**
     * 数据写入队列
     */
    private BlockingQueue<DataPacket> writeQueue;

    /**
     * 当前文件输出流
     */
    private volatile BufferedOutputStream currentOutputStream;

    /**
     * 当前文件
     */
    private volatile File currentFile;

    /**
     * 当前日期（用于跨天检测）
     */
    private volatile String currentDate;

    /**
     * 写入线程
     */
    private Thread writerThread;

    /**
     * 监控线程
     */
    private Thread monitorThread;

    /**
     * 服务运行标志
     */
    private final AtomicBoolean running = new AtomicBoolean(false);

    /**
     * 文件操作锁
     */
    private final ReentrantLock fileLock = new ReentrantLock();

    /**
     * 日期格式化器
     */
    private DateTimeFormatter dateFormatter;

    /**
     * 时区
     */
    private ZoneId zoneId;

    /**
     * 最后刷新时间
     */
    private volatile long lastFlushTime = 0;

    /**
     * 最后写入时间
     */
    private volatile long lastWriteTime = 0;

    // ==================== Setter 注入 ====================

    /**
     * Setter 注入配置属性
     *
     * @param properties GNSS录制配置属性
     */
    @Autowired
    public void setProperties(GnssRecorderProperties properties) {
        this.properties = properties;
    }

    // ==================== 初始化 ====================

    @PostConstruct
    public void init() {
        logger.info("========================================");
        logger.info("GNSS 数据录制服务初始化开始");
        logger.info("========================================");

        // 检查配置是否注入成功
        if (properties == null) {
            logger.error("GnssRecorderProperties 注入失败，服务无法启动");
            return;
        }

        try {
            // 初始化时区
            initTimezone();

            // 初始化目录
            initDirectory();

            // 初始化队列
            initQueue();

            // 启动服务
            startService();

            logger.info("GNSS 数据录制服务初始化完成");
            logger.info("  - 存储目录: {}", properties.getSaveDir());
            logger.info("  - 站点ID: {}", properties.getStationId());
            logger.info("  - 时区: {}", properties.getTimezone());

        } catch (Exception e) {
            logger.error("GNSS 数据录制服务初始化失败: {}", e.getMessage(), e);
            healthStatus.recordError("初始化失败: " + e.getMessage());
        }
    }

    /**
     * 初始化时区
     */
    private void initTimezone() {
        try {
            this.zoneId = ZoneId.of(properties.getTimezone());
            this.dateFormatter = DateTimeFormatter.ofPattern(properties.getDateFormat());
            TimeZone.setDefault(TimeZone.getTimeZone(zoneId));
            logger.info("时区设置: {}", zoneId);
        } catch (Exception e) {
            logger.warn("时区设置失败，使用系统默认时区: {}", e.getMessage());
            this.zoneId = ZoneId.systemDefault();
            this.dateFormatter = DateTimeFormatter.ofPattern(properties.getDateFormat());
        }
    }

    /**
     * 初始化存储目录
     */
    private void initDirectory() {
        File baseDir = new File(properties.getSaveDir());
        if (!baseDir.exists()) {
            boolean created = baseDir.mkdirs();
            if (created) {
                logger.info("创建存储目录: {}", baseDir.getAbsolutePath());
            } else {
                throw new RuntimeException("无法创建存储目录: " + baseDir.getAbsolutePath());
            }
        }

        // 检查目录权限
        if (!baseDir.canWrite()) {
            throw new RuntimeException("存储目录不可写: " + baseDir.getAbsolutePath());
        }

        // 检查磁盘空间
        checkDiskSpace();
    }

    /**
     * 初始化写入队列
     */
    private void initQueue() {
        this.writeQueue = new LinkedBlockingQueue<>(properties.getQueueCapacity());
        logger.info("写入队列容量: {}", properties.getQueueCapacity());
    }

    /**
     * 启动服务
     */
    private void startService() {
        if (!running.compareAndSet(false, true)) {
            logger.warn("服务已在运行中");
            return;
        }

        // 初始化当前日期
        this.currentDate = getCurrentDate();

        // 创建初始文件
        try {
            createNewFile();
        } catch (Exception e) {
            logger.error("创建初始文件失败: {}", e.getMessage());
            throw new RuntimeException("创建初始文件失败", e);
        }

        // 启动写入线程
        startWriterThread();

        // 启动监控线程
        startMonitorThread();

        // 标记服务启动
        healthStatus.markStarted();

        logger.info("GNSS 数据录制服务已启动");
    }

    // ==================== 数据接收接口 ====================

    @Override
    public void onNmeaReceived(String nmea) {
        if (properties == null || !properties.isRecordNmea() || !running.get()) {
            return;
        }

        if (nmea == null || nmea.isEmpty()) {
            return;
        }

        try {
            byte[] data = (nmea + "\r\n").getBytes(StandardCharsets.US_ASCII);
            DataPacket packet = new DataPacket(data, DataType.NMEA);
            if (!writeQueue.offer(packet, 100, TimeUnit.MILLISECONDS)) {
                logger.warn("NMEA 数据入队超时，队列已满");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("NMEA 数据入队异常: {}", e.getMessage());
        }
    }

    @Override
    public void onRtcmReceived(byte[] rtcmData) {
        if (properties == null || !properties.isRecordRtcm() || !running.get()) {
            return;
        }

        if (rtcmData == null || rtcmData.length == 0) {
            return;
        }

        try {
            DataPacket packet = new DataPacket(rtcmData.clone(), DataType.RTCM);
            if (!writeQueue.offer(packet, 100, TimeUnit.MILLISECONDS)) {
                logger.warn("RTCM 数据入队超时，队列已满");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (Exception e) {
            logger.error("RTCM 数据入队异常: {}", e.getMessage());
        }
    }

    // ==================== 写入线程 ====================

    /**
     * 启动写入线程
     */
    private void startWriterThread() {
        writerThread = new Thread(this::writerLoop, properties.getWriterThreadName());
        writerThread.setDaemon(false);
        writerThread.setUncaughtExceptionHandler((t, e) -> {
            logger.error("写入线程异常退出: {}", e.getMessage(), e);
            healthStatus.recordError("写入线程异常: " + e.getMessage());
        });
        writerThread.start();
        logger.info("写入线程已启动: {}", properties.getWriterThreadName());
    }

    /**
     * 写入线程主循环
     */
    private void writerLoop() {
        logger.info("写入线程开始运行");

        while (running.get()) {
            try {
                // 检查是否需要跨天切换
                checkDaySwitch();

                // 从队列获取数据
                DataPacket packet = writeQueue.poll(100, TimeUnit.MILLISECONDS);
                if (packet == null) {
                    // 队列为空，检查是否需要刷新
                    checkAndFlush();
                    continue;
                }

                // 写入数据
                writePacket(packet);

            } catch (InterruptedException e) {
                logger.info("写入线程被中断");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("写入数据异常: {}", e.getMessage());
                healthStatus.recordError("写入异常: " + e.getMessage());
                handleWriteError(e);
            }
        }

        // 线程退出前刷新并关闭文件
        flushAndClose();
        logger.info("写入线程已退出");
    }

    /**
     * 写入数据包
     */
    private void writePacket(DataPacket packet) {
        if (currentOutputStream == null) {
            logger.warn("输出流为空，尝试重新创建文件");
            try {
                createNewFile();
            } catch (Exception e) {
                logger.error("重新创建文件失败: {}", e.getMessage());
                healthStatus.recordWriteFailure();
                return;
            }
        }

        fileLock.lock();
        try {
            // 重试写入
            int retryCount = 0;
            while (retryCount < properties.getWriteRetryCount()) {
                try {
                    currentOutputStream.write(packet.getData());
                    lastWriteTime = System.currentTimeMillis();

                    // 更新统计
                    healthStatus.recordDataReceived(packet.getData().length, packet.getType() == DataType.RTCM);
                    healthStatus.recordDataWritten(packet.getData().length);

                    // 检查是否需要刷新
                    checkAndFlush();

                    break;

                } catch (IOException e) {
                    retryCount++;
                    logger.warn("写入失败，第 {} 次重试: {}", retryCount, e.getMessage());

                    if (retryCount >= properties.getWriteRetryCount()) {
                        healthStatus.recordWriteFailure();
                        throw e;
                    }

                    // 重试前等待
                    try {
                        Thread.sleep(properties.getWriteRetryDelayMs());
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException("写入被中断", ie);
                    }

                    // 尝试重新创建文件
                    try {
                        closeCurrentFile();
                        createNewFile();
                    } catch (Exception ex) {
                        logger.error("重新创建文件失败: {}", ex.getMessage());
                    }
                }
            }

        } catch (Exception e) {
            logger.error("写入数据包失败: {}", e.getMessage());
            healthStatus.recordError("写入失败: " + e.getMessage());
        } finally {
            fileLock.unlock();
        }
    }

    /**
     * 检查并刷新缓冲区
     */
    private void checkAndFlush() {
        long now = System.currentTimeMillis();

        // 条件1：距离上次刷新超过 flushIntervalMs
        // 条件2：距离上次刷新超过 maxFlushIntervalMs（强制刷新）
        if (now - lastFlushTime >= properties.getFlushIntervalMs() ||
                now - lastFlushTime >= properties.getMaxFlushIntervalMs()) {

            fileLock.lock();
            try {
                if (currentOutputStream != null) {
                    currentOutputStream.flush();
                    lastFlushTime = now;
                    logger.debug("缓冲区已刷新");
                }
            } catch (IOException e) {
                logger.error("刷新缓冲区失败: {}", e.getMessage());
            } finally {
                fileLock.unlock();
            }
        }
    }

    // ==================== 跨天切换 ====================

    /**
     * 检查是否需要跨天切换
     */
    private void checkDaySwitch() {
        String today = getCurrentDate();
        if (!today.equals(currentDate)) {
            logger.info("检测到日期变化: {} -> {}", currentDate, today);
            switchToNewDay(today);
        }
    }

    /**
     * 切换到新一天的文件
     */
    private void switchToNewDay(String newDate) {
        fileLock.lock();
        try {
            logger.info("开始跨天文件切换...");

            // 1. 刷新并关闭当前文件
            flushAndCloseFile();

            // 2. 更新日期
            String oldDate = this.currentDate;
            this.currentDate = newDate;

            // 3. 创建新文件
            createNewFile();

            // 4. 更新统计
            healthStatus.recordDaySwitch();
            healthStatus.recordFileSwitch(currentFile.getAbsolutePath(), newDate);

            logger.info("跨天文件切换完成: {} -> {}", oldDate, newDate);

        } catch (Exception e) {
            logger.error("跨天文件切换失败: {}", e.getMessage(), e);
            healthStatus.recordError("跨天切换失败: " + e.getMessage());

            // 尝试恢复
            try {
                createNewFile();
            } catch (Exception ex) {
                logger.error("恢复创建文件失败: {}", ex.getMessage());
            }
        } finally {
            fileLock.unlock();
        }
    }

    // ==================== 文件管理 ====================

    /**
     * 创建新文件
     */
    private void createNewFile() throws IOException {
        fileLock.lock();
        try {
            // 构建文件路径
            String dateStr = currentDate;
            File targetDir;

            if (properties.isDateSubdirectory()) {
                // 按日期创建子目录
                targetDir = new File(properties.getSaveDir(), dateStr);
            } else {
                targetDir = new File(properties.getSaveDir());
            }

            // 确保目录存在
            if (!targetDir.exists()) {
                boolean created = targetDir.mkdirs();
                if (!created && !targetDir.exists()) {
                    throw new IOException("无法创建目录: " + targetDir.getAbsolutePath());
                }
            }

            // 构建文件名
            String fileName = properties.getFilePrefix() + dateStr + properties.getFileSuffix();
            File newFile = new File(targetDir, fileName);

            // 如果文件已存在，追加模式
            boolean append = newFile.exists();

            // 创建输出流
            FileOutputStream fos = new FileOutputStream(newFile, append);
            this.currentOutputStream = new BufferedOutputStream(fos, properties.getBufferSize());
            this.currentFile = newFile;

            // 更新状态
            healthStatus.recordFileSwitch(newFile.getAbsolutePath(), currentDate);
            lastFlushTime = System.currentTimeMillis();

            logger.info("创建录制文件: {} (追加模式: {})", newFile.getAbsolutePath(), append);

            // 如果是追加模式且文件有内容，写入分隔标记
            if (append && newFile.length() > 0 && properties.isMarkInterruption()) {
                writeResumptionMarker();
            }

        } finally {
            fileLock.unlock();
        }
    }

    /**
     * 写入恢复标记
     */
    private void writeResumptionMarker() {
        if (currentOutputStream == null) {
            return;
        }

        try {
            String marker = String.format("\n# RESUMPTION: %s\n",
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()));
            currentOutputStream.write(marker.getBytes(StandardCharsets.UTF_8));
            currentOutputStream.flush();
            logger.info("写入恢复标记");
        } catch (IOException e) {
            logger.warn("写入恢复标记失败: {}", e.getMessage());
        }
    }

    /**
     * 写入中断标记
     */
    private void writeInterruptionMarker(String reason) {
        if (!properties.isMarkInterruption() || currentOutputStream == null) {
            return;
        }

        try {
            String marker = String.format("\n# INTERRUPTION START: %s - %s\n",
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()),
                    reason != null ? reason : "Unknown");
            currentOutputStream.write(marker.getBytes(StandardCharsets.UTF_8));
            currentOutputStream.flush();
        } catch (IOException e) {
            logger.warn("写入中断标记失败: {}", e.getMessage());
        }
    }

    /**
     * 写入中断恢复标记
     */
    private void writeInterruptionEndMarker(long duration) {
        if (!properties.isMarkInterruption() || currentOutputStream == null) {
            return;
        }

        try {
            String marker = String.format("\n# INTERRUPTION END: %s - Duration: %d ms\n",
                    new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS").format(new Date()),
                    duration);
            currentOutputStream.write(marker.getBytes(StandardCharsets.UTF_8));
            currentOutputStream.flush();
        } catch (IOException e) {
            logger.warn("写入中断结束标记失败: {}", e.getMessage());
        }
    }

    /**
     * 关闭当前文件
     */
    private void closeCurrentFile() {
        fileLock.lock();
        try {
            if (currentOutputStream != null) {
                try {
                    currentOutputStream.flush();
                    currentOutputStream.close();
                    logger.info("关闭录制文件: {}", currentFile.getAbsolutePath());
                } catch (IOException e) {
                    logger.error("关闭文件失败: {}", e.getMessage());
                } finally {
                    currentOutputStream = null;
                    currentFile = null;
                }
            }
        } finally {
            fileLock.unlock();
        }
    }

    /**
     * 刷新并关闭当前文件
     */
    private void flushAndCloseFile() {
        fileLock.lock();
        try {
            if (currentOutputStream != null) {
                try {
                    currentOutputStream.flush();
                    currentOutputStream.close();
                    logger.info("文件已刷新并关闭: {}", currentFile != null ? currentFile.getAbsolutePath() : "null");
                } catch (IOException e) {
                    logger.error("刷新关闭文件失败: {}", e.getMessage());
                } finally {
                    currentOutputStream = null;
                }
            }
        } finally {
            fileLock.unlock();
        }
    }

    /**
     * 刷新并关闭（服务停止时调用）
     */
    private void flushAndClose() {
        // 处理队列中剩余数据
        logger.info("处理队列中剩余数据...");
        DataPacket packet;
        int remainingCount = 0;
        while ((packet = writeQueue.poll()) != null) {
            try {
                writePacket(packet);
                remainingCount++;
            } catch (Exception e) {
                logger.error("处理剩余数据失败: {}", e.getMessage());
            }
        }
        logger.info("已处理 {} 条剩余数据", remainingCount);

        // 关闭文件
        flushAndCloseFile();
    }

    // ==================== 监控线程 ====================

    /**
     * 启动监控线程
     */
    private void startMonitorThread() {
        monitorThread = new Thread(this::monitorLoop, properties.getMonitorThreadName());
        monitorThread.setDaemon(true);
        monitorThread.setUncaughtExceptionHandler((t, e) -> {
            logger.error("监控线程异常退出: {}", e.getMessage(), e);
        });
        monitorThread.start();
        logger.info("监控线程已启动: {}", properties.getMonitorThreadName());
    }

    /**
     * 监控线程主循环
     */
    private void monitorLoop() {
        logger.info("监控线程开始运行");

        long lastDiskCheckTime = 0;

        while (running.get()) {
            try {
                Thread.sleep(properties.getHealthCheckIntervalMs());

                // 1. 检查数据中断
                checkDataInterruption();

                // 2. 检查磁盘空间
                long now = System.currentTimeMillis();
                if (now - lastDiskCheckTime >= properties.getDiskSpaceCheckIntervalMs()) {
                    checkDiskSpace();
                    lastDiskCheckTime = now;
                }

                // 3. 检查跨天（备用检查，防止写入线程异常导致跨天失败）
                checkDaySwitch();

                // 4. 输出健康状态日志
                if (properties.isDetailedHealthLog()) {
                    logger.debug("健康状态: {}", healthStatus);
                }

            } catch (InterruptedException e) {
                logger.info("监控线程被中断");
                Thread.currentThread().interrupt();
                break;
            } catch (Exception e) {
                logger.error("监控线程异常: {}", e.getMessage());
            }
        }

        logger.info("监控线程已退出");
    }

    /**
     * 检查数据中断
     */
    private void checkDataInterruption() {
        long timeSinceLastData = healthStatus.getTimeSinceLastData();

        if (timeSinceLastData > properties.getInterruptionThresholdMs()) {
            // 进入中断状态
            if (!healthStatus.isInterrupted()) {
                String reason = "数据中断";
                healthStatus.startInterruption(reason);
                writeInterruptionMarker(reason);
                logger.warn("检测到数据中断，距上次数据: {} ms", timeSinceLastData);
            }

            // 检查中断级别
            if (timeSinceLastData > properties.getInterruptionCriticalMs()) {
                logger.error("严重数据中断！距上次数据: {} ms", timeSinceLastData);
            } else if (timeSinceLastData > properties.getInterruptionWarningMs()) {
                logger.warn("数据中断告警，距上次数据: {} ms", timeSinceLastData);
            }

        } else {
            // 恢复正常
            if (healthStatus.isInterrupted()) {
                long duration = healthStatus.getCurrentInterruptionDuration();
                healthStatus.endInterruption();
                writeInterruptionEndMarker(duration);
                logger.info("数据恢复正常，中断时长: {} ms", duration);
            }
        }
    }

    /**
     * 检查磁盘空间
     */
    private void checkDiskSpace() {
        File dir = new File(properties.getSaveDir());
        if (!dir.exists()) {
            return;
        }

        long freeSpace = dir.getUsableSpace();
        long freeMb = freeSpace / (1024 * 1024);

        if (freeMb < properties.getMinDiskSpaceMb()) {
            logger.error("磁盘空间不足！剩余: {} MB，最小要求: {} MB",
                    freeMb, properties.getMinDiskSpaceMb());
            healthStatus.setDiskSpaceLow(true);
            healthStatus.setHealthy(false);
        } else {
            if (healthStatus.isDiskSpaceLow()) {
                logger.info("磁盘空间恢复正常: {} MB", freeMb);
                healthStatus.setDiskSpaceLow(false);
                healthStatus.setHealthy(true);
            }
        }
    }

    // ==================== 错误处理 ====================

    /**
     * 处理写入错误
     */
    private void handleWriteError(Exception e) {
        healthStatus.recordWriteFailure();

        // 尝试恢复
        try {
            closeCurrentFile();
            Thread.sleep(1000);
            createNewFile();
            logger.info("写入错误恢复成功");
        } catch (Exception ex) {
            logger.error("写入错误恢复失败: {}", ex.getMessage());
            healthStatus.setHealthy(false);
        }
    }

    // ==================== 工具方法 ====================

    /**
     * 获取当前日期字符串
     */
    private String getCurrentDate() {
        return LocalDate.now(zoneId).format(dateFormatter);
    }

    // ==================== 服务控制 ====================

    /**
     * 停止录制服务
     */
    @PreDestroy
    public void stop() {
        logger.info("========================================");
        logger.info("GNSS 数据录制服务正在停止...");
        logger.info("========================================");

        running.set(false);

        // 中断写入线程
        if (writerThread != null && writerThread.isAlive()) {
            writerThread.interrupt();
            try {
                writerThread.join(5000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // 中断监控线程
        if (monitorThread != null && monitorThread.isAlive()) {
            monitorThread.interrupt();
            try {
                monitorThread.join(2000);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        // 标记服务停止
        healthStatus.markStopped();

        logger.info("GNSS 数据录制服务已停止");
        logger.info("最终统计: {}", healthStatus);
    }

    // ==================== 公共接口 ====================

    /**
     * 获取健康状态
     */
    public RecorderHealthStatus getHealthStatus() {
        return healthStatus;
    }

    /**
     * 检查服务是否正在运行
     */
    public boolean isRunning() {
        return running.get() && healthStatus.isRunning();
    }

    /**
     * 检查服务是否健康
     */
    public boolean isHealthy() {
        return healthStatus.isHealthy();
    }

    /**
     * 获取当前文件路径
     */
    public String getCurrentFilePath() {
        return healthStatus.getCurrentFilePath();
    }

    /**
     * 获取统计信息
     */
    public String getStatistics() {
        return healthStatus.toString();
    }

    // ==================== 内部类 ====================

    /**
     * 数据包
     */
    private static class DataPacket {
        private final byte[] data;
        private final DataType type;
        private final long timestamp;

        public DataPacket(byte[] data, DataType type) {
            this.data = data;
            this.type = type;
            this.timestamp = System.currentTimeMillis();
        }

        public byte[] getData() {
            return data;
        }

        public DataType getType() {
            return type;
        }

        public long getTimestamp() {
            return timestamp;
        }
    }

    /**
     * 数据类型
     */
    private enum DataType {
        RTCM,
        NMEA
    }
}
