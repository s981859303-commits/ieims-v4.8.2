package com.ruoyi.gnss.domain;

import java.util.Date;

/**
 * 卫星观测数据实体类
 * 融合 GSV 和 RTCM 数据的完整卫星观测记录
 *
 * 数据来源：
 * - GSV（GPGSV/GBGSV）：仰角、方位角、信噪比
 * - RTCM（1074/1127）：伪距P1、相位L1、伪距P2、相位P2、信噪比
 *
 * @author GNSS Team
 * @date 2026-03-25
 */
public class SatObservation {

    /** 系统时间戳（毫秒） */
    private Long timestamp;

    /** 历元时间（GNSS时间，毫秒） */
    private Long epochTime;

    /** 卫星编号（统一格式：G01-G32表示GPS，C01-C63表示北斗） */
    private String satNo;

    /** 卫星系统（GPS/BDS） */
    private String satSystem;

    /** 仰角（度，范围0-90） */
    private Double elevation;

    /** 方位角（度，范围0-360） */
    private Double azimuth;

    /** 信噪比（dB-Hz） */
    private Double snr;

    /** 伪距P1（米）- L1频点伪距 */
    private Double pseudorangeP1;

    /** 相位L1（周）- L1频点载波相位 */
    private Double phaseL1;

    /** 伪距P2（米）- L2频点伪距 */
    private Double pseudorangeP2;

    /** 相位P2（周）- L2频点载波相位 */
    private Double phaseP2;

    /** 信号代码1（如C1C、C1W等） */
    private String c1;

    /** 信号代码2（如C2W、C2L等） */
    private String c2;

    /** 数据来源标识（GSV/RTCM/FUSED） */
    private String dataSource;

    // ==================== 构造方法 ====================

    public SatObservation() {
    }

    public SatObservation(Long timestamp, String satNo) {
        this.timestamp = timestamp;
        this.satNo = satNo;
    }

    // ==================== Getter/Setter 方法 ====================

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Long getEpochTime() {
        return epochTime;
    }

    public void setEpochTime(Long epochTime) {
        this.epochTime = epochTime;
    }

    public String getSatNo() {
        return satNo;
    }

    public void setSatNo(String satNo) {
        this.satNo = satNo;
    }

    public String getSatSystem() {
        return satSystem;
    }

    public void setSatSystem(String satSystem) {
        this.satSystem = satSystem;
    }

    public Double getElevation() {
        return elevation;
    }

    public void setElevation(Double elevation) {
        this.elevation = elevation;
    }

    public Double getAzimuth() {
        return azimuth;
    }

    public void setAzimuth(Double azimuth) {
        this.azimuth = azimuth;
    }

    public Double getSnr() {
        return snr;
    }

    public void setSnr(Double snr) {
        this.snr = snr;
    }

    public Double getPseudorangeP1() {
        return pseudorangeP1;
    }

    public void setPseudorangeP1(Double pseudorangeP1) {
        this.pseudorangeP1 = pseudorangeP1;
    }

    public Double getPhaseL1() {
        return phaseL1;
    }

    public void setPhaseL1(Double phaseL1) {
        this.phaseL1 = phaseL1;
    }

    public Double getPseudorangeP2() {
        return pseudorangeP2;
    }

    public void setPseudorangeP2(Double pseudorangeP2) {
        this.pseudorangeP2 = pseudorangeP2;
    }

    public Double getPhaseP2() {
        return phaseP2;
    }

    public void setPhaseP2(Double phaseP2) {
        this.phaseP2 = phaseP2;
    }

    public String getC1() {
        return c1;
    }

    public void setC1(String c1) {
        this.c1 = c1;
    }

    public String getC2() {
        return c2;
    }

    public void setC2(String c2) {
        this.c2 = c2;
    }

    public String getDataSource() {
        return dataSource;
    }

    public void setDataSource(String dataSource) {
        this.dataSource = dataSource;
    }

    // ==================== 辅助方法 ====================

    /**
     * 判断是否有GSV数据
     */
    public boolean hasGsvData() {
        return elevation != null || azimuth != null || snr != null;
    }

    /**
     * 判断是否有RTCM数据
     */
    public boolean hasRtcmData() {
        return pseudorangeP1 != null || phaseL1 != null ||
                pseudorangeP2 != null || phaseP2 != null;
    }

    /**
     * 判断是否为完整记录（同时包含GSV和RTCM数据）
     */
    public boolean isComplete() {
        return hasGsvData() && hasRtcmData();
    }

    @Override
    public String toString() {
        return "SatObservation{" +
                "timestamp=" + timestamp +
                ", epochTime=" + epochTime +
                ", satNo='" + satNo + '\'' +
                ", satSystem='" + satSystem + '\'' +
                ", elevation=" + elevation +
                ", azimuth=" + azimuth +
                ", snr=" + snr +
                ", pseudorangeP1=" + pseudorangeP1 +
                ", phaseL1=" + phaseL1 +
                ", pseudorangeP2=" + pseudorangeP2 +
                ", phaseP2=" + phaseP2 +
                ", dataSource='" + dataSource + '\'' +
                '}';
    }
}
