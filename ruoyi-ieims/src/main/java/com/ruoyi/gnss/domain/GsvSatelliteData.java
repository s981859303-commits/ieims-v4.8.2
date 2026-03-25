package com.ruoyi.gnss.domain;

/**
 * GSV 卫星数据实体类
 * 用于存储从 GPGSV/GBGSV 语句解析的卫星数据
 *
 * GSV 语句格式示例：
 * $GPGSV,3,1,12,01,45,123,42,02,38,089,38,03,22,234,40,04,15,180,36*7A
 *
 * 字段说明：
 * - 01: 卫星PRN号
 * - 45: 仰角（度）
 * - 123: 方位角（度）
 * - 42: 信噪比（dB-Hz）
 *
 * @author GNSS Team
 * @date 2026-03-25
 */
public class GsvSatelliteData {

    /** 卫星系统（GPS/BDS） */
    private String satSystem;

    /** 卫星PRN号（原始值） */
    private Integer prn;

    /** 统一卫星编号（G01/C01格式） */
    private String satNo;

    /** 仰角（度，范围0-90） */
    private Double elevation;

    /** 方位角（度，范围0-360） */
    private Double azimuth;

    /** 信噪比（dB-Hz） */
    private Double snr;

    /** 解析时间戳 */
    private Long timestamp;

    /** GSV语句序号（第几条GSV语句） */
    private Integer sentenceNumber;

    /** GSV语句总数 */
    private Integer totalSentences;

    /** 本语句可见卫星总数 */
    private Integer totalSats;

    // ==================== 构造方法 ====================

    public GsvSatelliteData() {
    }

    public GsvSatelliteData(String satSystem, Integer prn) {
        this.satSystem = satSystem;
        this.prn = prn;
    }

    // ==================== Getter/Setter 方法 ====================

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

    public String getSatNo() {
        return satNo;
    }

    public void setSatNo(String satNo) {
        this.satNo = satNo;
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

    public Long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(Long timestamp) {
        this.timestamp = timestamp;
    }

    public Integer getSentenceNumber() {
        return sentenceNumber;
    }

    public void setSentenceNumber(Integer sentenceNumber) {
        this.sentenceNumber = sentenceNumber;
    }

    public Integer getTotalSentences() {
        return totalSentences;
    }

    public void setTotalSentences(Integer totalSentences) {
        this.totalSentences = totalSentences;
    }

    public Integer getTotalSats() {
        return totalSats;
    }

    public void setTotalSats(Integer totalSats) {
        this.totalSats = totalSats;
    }

    // ==================== 辅助方法 ====================

    /**
     * 判断是否有有效数据
     */
    public boolean hasValidData() {
        return prn != null && prn > 0;
    }

    /**
     * 判断是否有位置信息
     */
    public boolean hasPositionInfo() {
        return elevation != null && azimuth != null;
    }

    /**
     * 判断是否有信号信息
     */
    public boolean hasSignalInfo() {
        return snr != null && snr > 0;
    }

    @Override
    public String toString() {
        return "GsvSatelliteData{" +
                "satSystem='" + satSystem + '\'' +
                ", prn=" + prn +
                ", satNo='" + satNo + '\'' +
                ", elevation=" + elevation +
                ", azimuth=" + azimuth +
                ", snr=" + snr +
                '}';
    }
}
