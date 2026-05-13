package com.ruoyi.ieims.gnss.service;

import com.ruoyi.system.service.ISysConfigService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 原始数据落盘服务 (7x24小时生产级优化版)
 * 具备内存缓冲、后台定时刷盘、跨天平滑切换、闲置句柄回收功能
 */
@Service
public class RawDataRecordService {

    private static final Logger logger = LoggerFactory.getLogger(RawDataRecordService.class);

    @Autowired(required = false)
    private ISysConfigService sysConfigService;

    private final Map<String, FileStreamWrapper> fileStreamCache = new ConcurrentHashMap<>();
    private volatile String currentDateStr;
    private volatile String cachedBasePath = null;

    // 定时任务调度器：负责定时刷盘和清理闲置文件
    private ScheduledExecutorService backgroundTaskScheduler;

    @PostConstruct
    public void init() {
        currentDateStr = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        // 初始化后台守护线程 (每 5 秒执行一次刷盘，每 60 秒清理一次闲置)
        backgroundTaskScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "GNSS-Disk-Flusher");
            t.setDaemon(true);
            return t;
        });

        backgroundTaskScheduler.scheduleAtFixedRate(this::flushAndCleanIdleStreams, 5, 5, TimeUnit.SECONDS);

        logger.info("✅ 原始数据落盘服务 (高性能优化版) 已启动...");
    }

    @PreDestroy
    public void destroy() {
        if (backgroundTaskScheduler != null) {
            backgroundTaskScheduler.shutdownNow();
        }
        for (FileStreamWrapper wrapper : fileStreamCache.values()) {
            wrapper.close();
        }
        fileStreamCache.clear();
        logger.info("🛑 原始数据落盘服务已安全关闭");
    }

    public void recordNmea(String stationId, String nmea) {
        if (nmea == null || nmea.isEmpty()) return;
        byte[] data = (nmea + "\r\n").getBytes(StandardCharsets.UTF_8);
        writeData(stationId, data);
    }

    public void recordRtcm(String stationId, byte[] rtcmData) {
        if (rtcmData == null || rtcmData.length == 0) return;
        writeData(stationId, rtcmData);
    }

    private void writeData(String stationId, byte[] data) {
        try {
            checkDayRollover();
            FileStreamWrapper wrapper = getOrCreateStream(stationId);
            if (wrapper != null) {
                wrapper.write(data);
            }
        } catch (Exception e) {
            logger.error("写入原始数据异常: stationId={}", stationId, e);
        }
    }

    /**
     * 跨天平滑切换：不再粗暴清空缓存，而是只更新日期标识
     * 具体的旧文件清理交给后台闲置回收线程去安全地处理
     */
    private void checkDayRollover() {
        String today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        if (!today.equals(currentDateStr)) {
            synchronized (this) {
                if (!today.equals(currentDateStr)) {
                    logger.info("⏳ 检测到跨天 ({} -> {})，准备切换日志文件", currentDateStr, today);
                    currentDateStr = today;
                    // 这里不直接 close，而是依赖 getOrCreateStream 创建新的一天的 key
                    // 昨天的 Stream 会因为不再被写入，在后台任务中被识别为“闲置”并安全关闭
                }
            }
        }
    }

    private FileStreamWrapper getOrCreateStream(String stationId) {
        String yearMonth = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyy-MM"));
        String dateStr = currentDateStr; // 使用当前类的缓存日期

        String basePath = getBasePath();
        String dirPath = basePath + yearMonth + File.separator;
        String fileName = stationId + "_" + dateStr + "_rawdata.log";
        String fullPath = dirPath + fileName;

        return fileStreamCache.computeIfAbsent(fullPath, path -> {
            try {
                File dir = new File(dirPath);
                if (!dir.exists()) dir.mkdirs();

                logger.info("📄 建立并打开日志缓冲流: {}", fileName);
                return new FileStreamWrapper(new File(path));
            } catch (Exception e) {
                logger.error("创建日志文件失败: {}", path, e);
                return null;
            }
        });
    }

    /**
     * 后台定时任务：执行刷盘与闲置资源清理
     */
    private void flushAndCleanIdleStreams() {
        long now = System.currentTimeMillis();
        long idleThreshold = 10 * 60 * 1000L; // 10分钟没有数据写入，视为闲置流

        Iterator<Map.Entry<String, FileStreamWrapper>> iterator = fileStreamCache.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, FileStreamWrapper> entry = iterator.next();
            FileStreamWrapper wrapper = entry.getValue();

            // 1. 定时将缓冲区数据刷入磁盘
            wrapper.flush();

            // 2. 如果文件流闲置超过 10 分钟（比如基站掉线，或者跨天了昨天旧文件没人写了）
            if (now - wrapper.getLastWriteTime() > idleThreshold) {
                logger.info("♻️ 清理闲置日志文件流: {}", entry.getKey());
                wrapper.close();
                iterator.remove(); // 安全地从 ConcurrentHashMap 中移除
            }
        }
    }

    private String getBasePath() {
        if (cachedBasePath != null) return cachedBasePath;

        String path = null;
        if (sysConfigService != null) {
            try {
                path = sysConfigService.selectConfigByKey("sys.rawdataPath");
            } catch (Exception e) {
                logger.warn("读取存储路径配置异常", e);
            }
        }
        if (path == null || path.trim().isEmpty()) path = "D:\\ieims\\logdata";
        if (!path.endsWith("\\") && !path.endsWith("/")) path = path + File.separator;

        cachedBasePath = path;
        return cachedBasePath;
    }

    /**
     * 高性能文件流包装器
     */
    private static class FileStreamWrapper {
        private final BufferedOutputStream bos;
        private final ReentrantLock lock = new ReentrantLock();
        private volatile long lastWriteTime;

        public FileStreamWrapper(File file) throws IOException {
            // 使用 BufferedOutputStream 提供 64KB 的内存缓冲区，极大缓解磁盘 IO 压力
            this.bos = new BufferedOutputStream(new FileOutputStream(file, true), 64 * 1024);
            this.lastWriteTime = System.currentTimeMillis();
        }

        public void write(byte[] data) {
            lock.lock();
            try {
                bos.write(data);
                this.lastWriteTime = System.currentTimeMillis();
                // ⚠️ 严禁在这里调用 flush()！交给后台线程去定时 flush！
            }
            catch (IOException e) { logger.error("写入缓冲流失败", e); }
            finally { lock.unlock(); }
        }

        public void flush() {
            lock.lock();
            try { if (bos != null) bos.flush(); }
            catch (IOException e) { logger.error("刷盘失败", e); }
            finally { lock.unlock(); }
        }

        public void close() {
            lock.lock();
            try {
                if (bos != null) {
                    bos.flush();
                    bos.close();
                }
            } catch (IOException e) { logger.error("关闭文件流失败", e); }
            finally { lock.unlock(); }
        }

        public long getLastWriteTime() {
            return lastWriteTime;
        }
    }
}