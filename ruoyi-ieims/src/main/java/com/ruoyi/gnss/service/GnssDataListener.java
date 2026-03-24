package com.ruoyi.gnss.service;

/**
 * GNSS 数据监听接口
 * 用于将底层拆包后的数据分发给其他服务
 */
public interface GnssDataListener {

    /**
     * 当收到完整的 NMEA 语句时回调
     * @param nmea 纯文本格式的 NMEA
     */
    void onNmeaReceived(String nmea);

    /**
     * 当收到完整的 RTCM3 二进制帧时回调
     * @param rtcmData 原始二进制数据
     */
    void onRtcmReceived(byte[] rtcmData);
}
