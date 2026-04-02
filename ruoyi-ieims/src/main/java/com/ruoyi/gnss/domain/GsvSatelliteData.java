package com.ruoyi.gnss.domain;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Objects;

/**
 * GSV 卫星数据实体类（增强版）
 *
 * 来源：GPGSV / GBGSV / GLGSV / GAGSV 语句
 *
 * GSV 语句格式：
 * $GPGSV,totalMsg,msgNum,satInView,prn,elev,azim,snr,...*CC
 *
 * 示例：
 * $GPGSV,3,1,12,01,45,123,42,02,38,089,38,03,22,234,40,04,15,180,36*7A
 *
 * 【重构说明】
 * 1. 新增 epochTime 字段，记录历元时间
 * 2. 新增 observationDate 字段，记录观测日期（来自ZDA）
 * 3. 新增 satUniqueKey 字段，作为唯一标识
 * 4. 新增 isValid 标志，标记数据有效性
 *
 * @version 2.0 - 2026-04-02 添加日期关联支持
 */
public class GsvSatelliteData {

    // ==================== 字段定义 ====================

    /** 卫星编号（统一格式：G01-G32表示GPS，C01-C63表示北斗，R01-R24表示GLONASS） */
    private String satNo;

    /** 卫星系统（GPS/BDS/GLONASS/GALILEO） */
    private String satSystem;

    /** 卫星PRN号（原始编号） */
    private Integer prn;

    /** 仰角（度，范围0-90） */
    private Double elevation;

    /** 方位角（度，范围0-360） */
    private Double azimuth;

    /** 信噪比（dB-Hz，范围0-99） */
    private Double snr;

    /** 历元时间（毫秒时间戳） */
    private Long epochTime;

    /** 观测日期（来自ZDA） */
    private LocalDate observationDate;

    /** 观测时间（时分秒毫秒） */
    private LocalTime observationTime;

    /** 卫星唯一标识（日期+时间+卫星编号） */
    private String satUniqueKey;

    /** 数据是否有效 */
    private boolean valid;

    /** GSV语句序号（第几条GSV） */
    private Integer msgNum;

    /** GSV总条数 */
    private Integer totalMsg;

    /** 可见卫星总数 */
    private Integer satInView;

    /** 原始语句 */
    private String rawSentence;

    /** 接收时间戳 */
    private Long receiveTimestamp;

    // ==================== 构造函数 ====================

    public GsvSatelliteData() {
        this.receiveTimestamp = System.currentTimeMillis();
        this.valid = true;
    }

    public GsvSatelliteData(String satNo, Double elevation, Double azimuth, Double snr) {
        this();
        this.satNo = satNo;
        this.elevation = elevation;
        this.azimuth = azimuth;
        this.snr = snr;
    }

    // ==================== 业务方法 ====================

    /**
     * 计算卫星唯一标识
     * 格式：日期_时间_卫星编号
     */
    public void calculateSatUniqueKey() {
        StringBuilder sb = new StringBuilder();

        if (observationDate != null) {
            sb.append(observationDate.toString().replace("-", ""));
        } else {
            sb.append("00000000");
        }

        sb.append("_");

        if (observationTime != null) {
            sb.append(observationTime.format(java.time.format.DateTimeFormatter.ofPattern("HHmmssSSS")));
        } else if (epochTime != null) {
            // 从历元时间提取时分秒
            long msInDay = epochTime % (24 * 60 * 60 * 1000);
            LocalTime time = LocalTime.ofNanoOfDay(msInDay * 1_000_000);
            sb.append(time.format(java.time.format.DateTimeFormatter.ofPattern("HHmmssSSS")));
        } else {
            sb.append("000000000");
        }

        sb.append("_");

        if (satNo != null) {
            sb.append(satNo);
        } else {
            sb.append("UNKNOWN");
        }

        this.satUniqueKey = sb.toString();
    }

    /**
     * 校验数据有效性
     */
    public void validate() {
        this.valid = true;

        // 校验卫星编号
        if (satNo == null || satNo.isEmpty()) {
            this.valid = false;
            return;
        }

        // 校验仰角范围
        if (elevation != null && (elevation < 0 || elevation > 90)) {
            this.valid = false;
            return;
        }

        // 校验方位角范围
        if (azimuth != null && (azimuth < 0 || azimuth > 360)) {
            this.valid = false;
            return;
        }

        // 校验信噪比范围
        if (snr != null && (snr < 0 || snr > 99)) {
            // 信噪比可能为空或超出范围，仅记录警告
            // 不标记为无效
        }
    }

    /**
     * 判断是否为高仰角卫星（仰角 > 30度）
     */
    public boolean isHighElevation() {
        return elevation != null && elevation > 30;
    }

    /**
     * 判断信号强度是否良好（SNR > 35 dB-Hz）
     */
    public boolean isGoodSignal() {
        return snr != null && snr > 35;
    }

    /**
     * 获取卫星状态描述
     */
    public String getStatusDescription() {
        if (!valid) {
            return "无效数据";
        }
        if (snr == null || snr < 20) {
            return "信号弱";
        }
        if (elevation != null && elevation < 15) {
            return "低仰角";
        }
        if (isGoodSignal() && isHighElevation()) {
            return "观测条件良好";
        }
        return "正常";
    }

    // ==================== Getters & Setters ====================

    public String getSatNo() {
        return satNo;
    }

    public void setSatNo(String satNo) {
        this.satNo = satNo;
        // 自动解析卫星系统
        if (satNo != null && !satNo.isEmpty()) {
            char prefix = satNo.charAt(0);
            switch (prefix) {
                case 'G':
                    this.satSystem = "GPS";
                    break;
                case 'C':
                    this.satSystem = "BDS";
                    break;
                case 'R':
                    this.satSystem = "GLONASS";
                    break;
                case 'E':
                    this.satSystem = "GALILEO";
                    break;
                case 'J':
                    this.satSystem = "QZSS";
                    break;
                case 'I':
                    this.satSystem = "IRNSS";
                    break;
                case 'S':
                    this.satSystem = "SBAS";
                    break;
                default:
                    this.satSystem = "UNKNOWN";
            }
        }
    }

    public String getSatSystem() {
        return satSystem;
    }

    public void setSatSystem(String satSystem) {
        this.satSystem = satSystem;
    }

    public Integer getPrn() {
        return prn;
    }

    public void setPrn(Integer prn) {
        this.prn = prn;
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

    public Long getEpochTime() {
        return epochTime;
    }

    public void setEpochTime(Long epochTime) {
        this.epochTime = epochTime;
    }

    public LocalDate getObservationDate() {
        return observationDate;
    }

    public void setObservationDate(LocalDate observationDate) {
        this.observationDate = observationDate;
    }

    public LocalTime getObservationTime() {
        return observationTime;
    }

    public void setObservationTime(LocalTime observationTime) {
        this.observationTime = observationTime;
    }

    public String getSatUniqueKey() {
        if (satUniqueKey == null) {
            calculateSatUniqueKey();
        }
        return satUniqueKey;
    }

    public void setSatUniqueKey(String satUniqueKey) {
        this.satUniqueKey = satUniqueKey;
    }

    public boolean isValid() {
        return valid;
    }

    public void setValid(boolean valid) {
        this.valid = valid;
    }

    public Integer getMsgNum() {
        return msgNum;
    }

    public void setMsgNum(Integer msgNum) {
        this.msgNum = msgNum;
    }

    public Integer getTotalMsg() {
        return totalMsg;
    }

    public void setTotalMsg(Integer totalMsg) {
        this.totalMsg = totalMsg;
    }

    public Integer getSatInView() {
        return satInView;
    }

    public void setSatInView(Integer satInView) {
        this.satInView = satInView;
    }

    public String getRawSentence() {
        return rawSentence;
    }

    public void setRawSentence(String rawSentence) {
        this.rawSentence = rawSentence;
    }

    public Long getReceiveTimestamp() {
        return receiveTimestamp;
    }

    public void setReceiveTimestamp(Long receiveTimestamp) {
        this.receiveTimestamp = receiveTimestamp;
    }

    // ==================== Object 方法 ====================

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        GsvSatelliteData that = (GsvSatelliteData) o;
        return Objects.equals(getSatUniqueKey(), that.getSatUniqueKey());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getSatUniqueKey());
    }

    @Override
    public String toString() {
        return "GsvSatelliteData{" +
                "satNo='" + satNo + '\'' +
                ", satSystem='" + satSystem + '\'' +
                ", elevation=" + elevation +
                ", azimuth=" + azimuth +
                ", snr=" + snr +
                ", observationDate=" + observationDate +
                ", valid=" + valid +
                '}';
    }
}
