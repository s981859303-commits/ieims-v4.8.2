package com.ruoyi.gnss.domain;

import java.util.Date;

/**
 * GNSS 解算结果实体类
 */
public class GnssSolution {

    private Date time;
    private double latitude;
    private double longitude;
    private double altitude;
    private int status;
    private int satelliteCount;
    private double hdop;
    private String solutionType;

    public GnssSolution() {}

    public GnssSolution(Date time, double latitude, double longitude, double altitude,
                        int status, int satelliteCount) {
        this.time = time;
        this.latitude = latitude;
        this.longitude = longitude;
        this.altitude = altitude;
        this.status = status;
        this.satelliteCount = satelliteCount;
        this.solutionType = getSolutionTypeByStatus(status);
    }

    // --- Getter 方法 ---
    public Date getTime() { return time; }
    public double getLatitude() { return latitude; }
    public double getLongitude() { return longitude; }
    public double getAltitude() { return altitude; }
    public int getStatus() { return status; }
    public int getSatelliteCount() { return satelliteCount; }
    public double getHdop() { return hdop; }
    public String getSolutionType() { return solutionType; }

    // --- Setter 方法 ---
    public GnssSolution setTime(Date time) { this.time = time; return this; }
    public GnssSolution setLatitude(double latitude) { this.latitude = latitude; return this; }
    public GnssSolution setLongitude(double longitude) { this.longitude = longitude; return this; }
    public GnssSolution setAltitude(double altitude) { this.altitude = altitude; return this; }
    public GnssSolution setStatus(int status) { this.status = status; this.solutionType = getSolutionTypeByStatus(status); return this; }
    public GnssSolution setSatelliteCount(int satelliteCount) { this.satelliteCount = satelliteCount; return this; }
    public GnssSolution setHdop(double hdop) { this.hdop = hdop; return this; }

    public static String getSolutionTypeByStatus(int status) {
        switch (status) {
            case 0: return "无效";
            case 1: return "单点定位";
            case 2: return "差分定位";
            case 4: return "固定解";
            case 5: return "浮点解";
            default: return "未知";
        }
    }

    public boolean isValid() {
        return status > 0 && satelliteCount > 0;
    }

    public boolean isHighPrecision() {
        return status == 4 || status == 5;
    }

    @Override
    public String toString() {
        return String.format("[时间:%s] 状态:%s(%d) | 经度:%.8f 纬度:%.8f 高程:%.3f | 卫星:%d | HDOP:%.2f",
                time, solutionType, status, longitude, latitude, altitude, satelliteCount, hdop);
    }
}
