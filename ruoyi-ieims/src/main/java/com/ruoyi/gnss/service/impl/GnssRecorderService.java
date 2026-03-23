package com.ruoyi.gnss.service.impl;

import com.ruoyi.gnss.service.GnssDataListener;
import org.springframework.stereotype.Service;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * GNSS 数据录制服务
 * 作用：将收到的 RTCM 原始数据自动保存为文件，方便后续科研分析 (PPK)
 */
//@Service
public class GnssRecorderService implements GnssDataListener {

    // 💾 数据保存路径 (请修改为你电脑的实际路径)
    private static final String SAVE_DIR = "D:\\GnssData\\";

    private FileOutputStream outputStream;
    private String currentFileName;

    @PostConstruct
    public void init() {
        // 确保存储目录存在
        new File(SAVE_DIR).mkdirs();
    }

    @Override
    public void onNmeaReceived(String nmea) {
        // 如果也想存 NMEA，可以在这里写 (通常存 RTCM 就够了)
    }

    @Override
    public synchronized void onRtcmReceived(byte[] rtcmData) {
        try {
            // 1. 检查是否需要创建新文件 (例如按小时或按天分文件)
            // 这里简单策略：每次启动存一个新文件，或者按文件名区分
            checkFileStream();

            // 2. 写入文件
            if (outputStream != null) {
                outputStream.write(rtcmData);
                // 实时流通常不需要频繁 flush，但在测试阶段可以加
                // outputStream.flush();
            }
        } catch (IOException e) {
            System.err.println("❌ [文件录制] 写入失败: " + e.getMessage());
        }
    }

    /**
     * 检查并创建文件流
     * 策略：如果流没打开，或者文件名需要按时间更新，则新建
     */
    private void checkFileStream() throws IOException {
        // 生成当前时间的文件名，例如：20260122_1400.rtcm
        SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd_HHmm");
        String timeStr = sdf.format(new Date());
        String newFileName = SAVE_DIR + "Record_" + timeStr + ".rtcm";

        // 如果还没打开流，或者你想每小时切分一个文件，可以在这里判断
        if (outputStream == null) {
            System.out.println("📼 [开始录制] 创建新数据文件: " + newFileName);
            outputStream = new FileOutputStream(newFileName, true); // true = 追加模式
            currentFileName = newFileName;
        }
    }

    @PreDestroy
    public void stopRecording() {
        try {
            if (outputStream != null) {
                outputStream.close();
                System.out.println("⏹️ [停止录制] 文件已保存: " + currentFileName);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}