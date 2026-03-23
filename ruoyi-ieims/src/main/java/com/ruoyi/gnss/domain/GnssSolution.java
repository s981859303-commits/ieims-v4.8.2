package com.ruoyi.gnss.domain;

import java.util.Date;

/**
 * GNSS 解算结果实体类
 */
public class GnssSolution {
    // 解算时间
    private Date time;

    // 纬度 (度)
    private double latitude;

    // 经度 (度)
    private double longitude;

    // 高程 (米)
    private double altitude;

    // 解算状态 (0=无效, 1=单点, 2=浮点, 4=固定, 5=浮点) -> 对应 NMEA 的 quality
    private int status;

    // 卫星数量
    private int satelliteCount;

    // ... 请自行添加 getter 和 setter 方法 ...
    // 为了演示方便，这里写一个 toString
    @Override
    public String toString() {
        return String.format("[时间:%s] 状态:%d | 经度:%.8f 纬度:%.8f 高程:%.3f | 卫星:%d",
                time, status, longitude, latitude, altitude, satelliteCount);
    }

    public void setTime(Date time) { this.time = time; }
    public void setLatitude(double latitude) { this.latitude = latitude; }
    public void setLongitude(double longitude) { this.longitude = longitude; }
    public void setAltitude(double altitude) { this.altitude = altitude; }
    public void setStatus(int status) { this.status = status; }
    public void setSatelliteCount(int satelliteCount) { this.satelliteCount = satelliteCount; }
}