package com.ruoyi.gnss.domain;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.Objects;

/**
 * 卫星观测数据实体类
 * 融合 GSV 、 ZDA和 RTCM 数据的完整卫星观测记录
 *
 * 数据来源：
 * - GSV（GPGSV/GBGSV）：仰角、方位角、信噪比
 * - RTCM（1074/1127）：伪距P1、相位L1、伪距P2、相位P2、信噪比
 * - ZDA（GNZDA/GPZDA/BDZDA）：观测日期（年月日）
 *
 * 【重构说明】
 * 1. 新增 observationDate 字段，存储ZDA解析的日期
 * 2. 新增 observationTime 字段，存储观测时间（时分秒毫秒）
 * 3. 新增 obsUniqueKey 字段，作为唯一标识（日期+时间+卫星编号）
 * 4. 新增完整时间戳计算方法，支持跨天场景
 *
 * @version 2.0 - 2026-04-02 添加日期关联支持
 */
public class SatObservation {

    // ==================== 原有字段 ====================

    /** 系统时间戳（毫秒）- 数据接收时间 */
    private Long timestamp;

    /** 历元时间（GNSS时间，毫秒）- 仅时分秒毫秒部分 */
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

    /** 数据来源（GSV/RTCM/FUSED） */
    private String dataSource;

    // ==================== 新增字段 ====================

    /** 观测日期（来自ZDA语句） */
    private LocalDate observationDate;

    /** 观测时间（时分秒毫秒） */
    private LocalTime obsTime;

    /** 唯一标识键（格式：日期_时间_卫星编号，如 2026-04-01_083015.000_G01） */
    private String obsUniqueKey;

    /** 完整时间戳（日期+时间，毫秒） */
    private Long fullTimestamp;

    /** 日期来源（ZDA/SYSTEM/MIGRATED） */
    private String dateSource;

    /** 站点ID */
    private String stationId;

    // ==================== 静态常量 ====================

    /** 日期格式化器 */
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /** 时间格式化器 */
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    /** 紧凑时间格式化器（用于生成唯一键） */
    private static final DateTimeFormatter COMPACT_TIME_FORMATTER = DateTimeFormatter.ofPattern("HHmmssSSS");

    /** 数据来源常量 */
    public static final String SOURCE_GSV = "GSV";
    public static final String SOURCE_RTCM = "RTCM";
    public static final String SOURCE_FUSED = "FUSED";

    /** 日期来源常量 */
    public static final String DATE_SOURCE_ZDA = "ZDA";
    public static final String DATE_SOURCE_SYSTEM = "SYSTEM";

    // ==================== 构造方法 ====================

    public SatObservation() {
        // 默认构造方法
    }

    /**
     * 完整构造方法
     */
    public SatObservation(Long timestamp, Long epochTime, String satNo, String satSystem,
                          Double elevation, Double azimuth, Double snr,
                          Double pseudorangeP1, Double phaseL1,
                          Double pseudorangeP2, Double phaseP2,
                          String c1, String c2, String dataSource) {
        this.timestamp = timestamp;
        this.epochTime = epochTime;
        this.satNo = satNo;
        this.satSystem = satSystem;
        this.elevation = elevation;
        this.azimuth = azimuth;
        this.snr = snr;
        this.pseudorangeP1 = pseudorangeP1;
        this.phaseL1 = phaseL1;
        this.pseudorangeP2 = pseudorangeP2;
        this.phaseP2 = phaseP2;
        this.c1 = c1;
        this.c2 = c2;
        this.dataSource = dataSource;
    }

    /**
     * 带日期的构造方法
     */
    public SatObservation(LocalDate observationDate, LocalTime observationTime, String satNo) {
        this.observationDate = observationDate;
        this.observationTime = observationTime;
        this.satNo = satNo;
        this.timestamp = System.currentTimeMillis();
        generateUniqueKey();
    }

    // ==================== 核心业务方法 ====================

    /**
     * 设置观测日期并计算完整时间戳
     *
     * @param date 观测日期
     * @param source 日期来源（ZDA/SYSTEM）
     */
    public void setObservationDate(LocalDate date, String source) {
        this.observationDate = date;
        this.dateSource = source;
        calculateFullTimestamp();
        generateUniqueKey();
    }

    /**
     * 设置观测时间并计算完整时间戳
     *
     * @param time 观测时间
     */
    public void setObservationTime(LocalTime time) {
        this.observationTime = time;
        calculateFullTimestamp();
        generateUniqueKey();
    }

    /**
     * 从历元时间（毫秒）解析观测时间
     *
     * @param epochTimeMs 历元时间（仅时分秒毫秒部分，毫秒）
     */
    public void setEpochTimeAndParseTime(Long epochTimeMs) {
        this.epochTime = epochTimeMs;
        if (epochTimeMs != null && epochTimeMs > 0) {
            // 从毫秒数解析时分秒
            long totalMs = epochTimeMs % (24 * 60 * 60 * 1000); // 确保在一天内
            int hours = (int) (totalMs / (60 * 60 * 1000));
            int minutes = (int) ((totalMs % (60 * 60 * 1000)) / (60 * 1000));
            int seconds = (int) ((totalMs % (60 * 1000)) / 1000);
            int millis = (int) (totalMs % 1000);

            this.observationTime = LocalTime.of(hours, minutes, seconds, millis * 1_000_000);
        }
        calculateFullTimestamp();
        generateUniqueKey();
    }

    /**
     * 计算完整时间戳
     * 将日期和时间组合成完整的时间戳
     */
    private void calculateFullTimestamp() {
        if (observationDate != null && observationTime != null) {
            try {
                ZonedDateTime zdt = ZonedDateTime.of(
                        observationDate, observationTime, ZoneOffset.UTC
                );
                this.fullTimestamp = zdt.toInstant().toEpochMilli();
            } catch (Exception e) {
                // 时间组合失败，使用系统时间戳
                this.fullTimestamp = this.timestamp;
            }
        } else {
            this.fullTimestamp = this.timestamp;
        }
    }

    /**
     * 生成唯一标识键
     * 格式：{日期}_{时间}_{卫星编号}
     */
    private void generateUniqueKey() {
        if (observationDate != null && observationTime != null && satNo != null) {
            this.obsUniqueKey = String.format("%s_%s_%s",
                    observationDate.format(DATE_FORMATTER),
                    observationTime.format(COMPACT_TIME_FORMATTER),
                    satNo
            );
        } else if (satNo != null && epochTime != null) {
            // 降级方案：使用历元时间和卫星编号
            this.obsUniqueKey = String.format("EPOCH_%d_%s", epochTime, satNo);
        }
    }

    /**
     * 获取日期字符串（用于数据库存储）
     */
    public String getObservationDateStr() {
        return observationDate != null ? observationDate.format(DATE_FORMATTER) : null;
    }

    /**
     * 获取时间字符串（用于数据库存储）
     */
    public String getObservationTimeStr() {
        return observationTime != null ? observationTime.format(TIME_FORMATTER) : null;
    }

    /**
     * 从ZonedDateTime设置日期和时间
     */
    public void setFromZonedDateTime(ZonedDateTime zdt) {
        if (zdt != null) {
            this.observationDate = zdt.toLocalDate();
            this.observationTime = zdt.toLocalTime();
            this.dateSource = DATE_SOURCE_ZDA;
            calculateFullTimestamp();
            generateUniqueKey();
        }
    }

    // ==================== 原有Getter/Setter方法 ====================

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
        setEpochTimeAndParseTime(epochTime);
    }

    public String getSatNo() {
        return satNo;
    }

    public void setSatNo(String satNo) {
        this.satNo = satNo;
        generateUniqueKey();
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

    // ==================== 新增字段Getter/Setter方法 ====================

    public LocalDate getObservationDate() {
        return observationDate;
    }

    public void setObservationDate(LocalDate observationDate) {
        this.observationDate = observationDate;
        calculateFullTimestamp();
        generateUniqueKey();
    }

    public LocalTime getObservationTime() {
        return observationTime;
    }

    public String getObsUniqueKey() {
        return obsUniqueKey;
    }

    public void setObsUniqueKey(String obsUniqueKey) {
        this.obsUniqueKey = obsUniqueKey;
    }

    public Long getFullTimestamp() {
        if (fullTimestamp == null) {
            calculateFullTimestamp();
        }
        return fullTimestamp;
    }

    public void setFullTimestamp(Long fullTimestamp) {
        this.fullTimestamp = fullTimestamp;
    }

    public String getDateSource() {
        return dateSource;
    }

    public void setDateSource(String dateSource) {
        this.dateSource = dateSource;
    }

    public String getStationId() {
        return stationId;
    }

    public void setStationId(String stationId) {
        this.stationId = stationId;
    }

    // ==================== 辅助判断方法 ====================

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

    /**
     * 判断是否有完整日期时间
     */
    public boolean hasCompleteDateTime() {
        return observationDate != null && observationTime != null;
    }

    /**
     * 判断日期是否来自ZDA
     */
    public boolean isDateFromZda() {
        return DATE_SOURCE_ZDA.equals(dateSource);
    }

    /**
     * 合并另一个观测数据（用于GSV和RTCM数据融合）
     */
    public void mergeFrom(SatObservation other) {
        if (other == null) return;

        // 如果当前没有日期，从other获取
        if (this.observationDate == null && other.observationDate != null) {
            this.observationDate = other.observationDate;
            this.dateSource = other.dateSource;
        }

        // 如果当前没有时间，从other获取
        if (this.observationTime == null && other.observationTime != null) {
            this.observationTime = other.observationTime;
        }

        // 合并GSV数据
        if (other.elevation != null) this.elevation = other.elevation;
        if (other.azimuth != null) this.azimuth = other.azimuth;
        if (other.snr != null) this.snr = other.snr;

        // 合并RTCM数据
        if (other.pseudorangeP1 != null) this.pseudorangeP1 = other.pseudorangeP1;
        if (other.phaseL1 != null) this.phaseL1 = other.phaseL1;
        if (other.pseudorangeP2 != null) this.pseudorangeP2 = other.pseudorangeP2;
        if (other.phaseP2 != null) this.phaseP2 = other.phaseP2;
        if (other.c1 != null) this.c1 = other.c1;
        if (other.c2 != null) this.c2 = other.c2;

        // 更新数据来源
        if (this.hasGsvData() && this.hasRtcmData()) {
            this.dataSource = SOURCE_FUSED;
        } else if (this.hasGsvData()) {
            this.dataSource = SOURCE_GSV;
        } else if (this.hasRtcmData()) {
            this.dataSource = SOURCE_RTCM;
        }

        // 重新计算时间戳和唯一键
        calculateFullTimestamp();
        generateUniqueKey();
    }

    /**
     * 创建副本
     */
    public SatObservation copy() {
        SatObservation copy = new SatObservation();
        copy.timestamp = this.timestamp;
        copy.epochTime = this.epochTime;
        copy.satNo = this.satNo;
        copy.satSystem = this.satSystem;
        copy.elevation = this.elevation;
        copy.azimuth = this.azimuth;
        copy.snr = this.snr;
        copy.pseudorangeP1 = this.pseudorangeP1;
        copy.phaseL1 = this.phaseL1;
        copy.pseudorangeP2 = this.pseudorangeP2;
        copy.phaseP2 = this.phaseP2;
        copy.c1 = this.c1;
        copy.c2 = this.c2;
        copy.dataSource = this.dataSource;
        copy.observationDate = this.observationDate;
        copy.observationTime = this.observationTime;
        copy.obsUniqueKey = this.obsUniqueKey;
        copy.fullTimestamp = this.fullTimestamp;
        copy.dateSource = this.dateSource;
        copy.stationId = this.stationId;
        return copy;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SatObservation that = (SatObservation) o;
        return Objects.equals(obsUniqueKey, that.obsUniqueKey);
    }

    @Override
    public int hashCode() {
        return Objects.hash(obsUniqueKey);
    }

    @Override
    public String toString() {
        return "SatObservation{" +
                "timestamp=" + timestamp +
                ", epochTime=" + epochTime +
                ", satNo='" + satNo + '\'' +
                ", satSystem='" + satSystem + '\'' +
                ", observationDate=" + getObservationDateStr() +
                ", observationTime=" + getObservationTimeStr() +
                ", obsUniqueKey='" + obsUniqueKey + '\'' +
                ", dateSource='" + dateSource + '\'' +
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
