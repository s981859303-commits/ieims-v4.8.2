package com.ruoyi.gnss.domain;

import java.util.Date;

/**
 * NMEA 数据记录实体
 * 用于在 Service 层和 DAO 层之间传输数据
 */
public class NmeaRecord {

    // 1. 接收时间 (系统时间，用于和 RTCM 对齐)
    private Date receivedTime;

    // 2. 设备时间 (从 NMEA 字符串里解析出来的 UTC 时间，可选)
    private Date deviceTime;

    // 3. 原始数据 (完整的 $GPGGA... 字符串)
    private String rawContent;

    // --- 构造方法 ---
    public NmeaRecord() {}

    public NmeaRecord(Date receivedTime, String rawContent) {
        this.receivedTime = receivedTime;
        this.rawContent = rawContent;
    }

    // --- Getter / Setter ---
    public Date getReceivedTime() { return receivedTime; }
    public void setReceivedTime(Date receivedTime) { this.receivedTime = receivedTime; }

    public Date getDeviceTime() { return deviceTime; }
    public void setDeviceTime(Date deviceTime) { this.deviceTime = deviceTime; }

    public String getRawContent() { return rawContent; }
    public void setRawContent(String rawContent) { this.rawContent = rawContent; }

    @Override
    public String toString() {
        return "NmeaRecord{Time=" + receivedTime + ", Content='" + rawContent + "'}";
    }
}