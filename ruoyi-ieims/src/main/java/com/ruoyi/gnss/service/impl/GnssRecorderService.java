package com.ruoyi.gnss.service.impl;

import com.ruoyi.gnss.service.GnssDataListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.locks.ReentrantLock;

/**
 * GNSS 数据录制服务
 */
@Service
public class GnssRecorderService implements GnssDataListener {

    private static final Logger logger = LoggerFactory.getLogger(GnssRecorderService.class);

    @Value("${gnss.recorder.enabled:false}")
    private boolean enabled;

    @Value("${gnss.recorder.saveDir:./gnss-data}")
    private String saveDir;

    @Value("${gnss.recorder.filePrefix:Record_}")
    private String filePrefix;

    @Value("${gnss.recorder.fileSuffix:.rtcm}")
    private String fileSuffix;

    @Value("${gnss.recorder.splitIntervalHours:1}")
    private int splitIntervalHours;

    @Value("${gnss.recorder.flushInterval:5000}")
    private int flushInterval;

    private FileOutputStream outputStream;
    private String currentFileName;
    private long currentFileStartTime;
    private final ReentrantLock writeLock = new ReentrantLock();
    private long lastFlushTime = 0;

    @PostConstruct
    public void init() {
        if (!enabled) {
            logger.info("GNSS 数据录制服务已禁用");
            return;
        }

        File dir = new File(saveDir);
        if (!dir.exists()) {
            boolean created = dir.mkdirs();
            if (created) {
                logger.info("创建数据存储目录: {}", saveDir);
            }
        }

        logger.info("GNSS 数据录制服务已初始化，存储路径: {}", saveDir);
    }

    @Override
    public void onNmeaReceived(String nmea) {
        // 不录制 NMEA 数据
    }

    @Override
    public void onRtcmReceived(byte[] rtcmData) {
        if (!enabled || rtcmData == null || rtcmData.length == 0) {
            return;
        }

        writeLock.lock();
        try {
            checkAndRotateFile();

            if (outputStream != null) {
                outputStream.write(rtcmData);

                long now = System.currentTimeMillis();
                if (now - lastFlushTime > flushInterval) {
                    outputStream.flush();
                    lastFlushTime = now;
                }
            }
        } catch (IOException e) {
            logger.error("写入 RTCM 数据失败: {}", e.getMessage());
            closeFileStream();
        } finally {
            writeLock.unlock();
        }
    }

    private void checkAndRotateFile() throws IOException {
        long now = System.currentTimeMillis();

        boolean needNewFile = (outputStream == null) ||
                (splitIntervalHours > 0 &&
                        now - currentFileStartTime > splitIntervalHours * 3600 * 1000L);

        if (needNewFile) {
            closeFileStream();
            createNewFile();
        }
    }

    private void createNewFile() throws IOException {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmmss");
        String timeStr = sdf.format(new Date());
        currentFileName = saveDir + File.separator + filePrefix + timeStr + fileSuffix;
        currentFileStartTime = System.currentTimeMillis();

        try {
            outputStream = new FileOutputStream(currentFileName, true);
            logger.info("创建新数据文件: {}", currentFileName);
        } catch (IOException e) {
            logger.error("创建数据文件失败: {}", currentFileName);
            throw e;
        }
    }

    private void closeFileStream() {
        if (outputStream != null) {
            try {
                outputStream.flush();
                outputStream.close();
                logger.info("关闭数据文件: {}", currentFileName);
            } catch (IOException e) {
                logger.error("关闭文件流异常: {}", e.getMessage());
            } finally {
                outputStream = null;
                currentFileName = null;
            }
        }
    }

    public boolean isRecording() {
        return enabled && outputStream != null;
    }

    public String getCurrentFileName() {
        return currentFileName;
    }

    @PreDestroy
    public void stopRecording() {
        logger.info("正在停止 GNSS 数据录制服务...");
        writeLock.lock();
        try {
            closeFileStream();
        } finally {
            writeLock.unlock();
        }
        logger.info("GNSS 数据录制服务已停止");
    }
}
