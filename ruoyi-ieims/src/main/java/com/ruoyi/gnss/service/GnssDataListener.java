package com.ruoyi.gnss.service;

/**
 * GNSS 数据监听接口
 * 用于将底层拆包后的数据分发给：数据库存储、RTKLIB解算、前端WebSocket推送等
 */
public interface GnssDataListener {

    /**
     * 当收到完整的 NMEA 语句时回调
     * @param nmea 纯文本格式的 NMEA (例如 "$GPGGA,123...")
     */
    void onNmeaReceived(String nmea);

    /**
     * 当收到完整的 RTCM3 二进制帧时回调
     * @param rtcmData 原始二进制数据 (以 D3 开头)
     */
    void onRtcmReceived(byte[] rtcmData);
}