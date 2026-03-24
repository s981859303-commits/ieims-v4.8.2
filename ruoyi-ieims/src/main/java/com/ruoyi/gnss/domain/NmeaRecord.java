package com.ruoyi.gnss.domain;

import java.util.Date;

/**
 * NMEA 数据记录实体
 */
public class NmeaRecord {

    private Date receivedTime;
    private Date deviceTime;
    private String rawContent;

    public NmeaRecord() {}

    public NmeaRecord(Date receivedTime, String rawContent) {
        this.receivedTime = receivedTime;
        this.rawContent = rawContent;
    }

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
