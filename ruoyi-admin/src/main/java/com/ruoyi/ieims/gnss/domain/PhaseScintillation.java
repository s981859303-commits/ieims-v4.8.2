package com.ruoyi.ieims.gnss.domain;

import java.io.Serializable;
import java.util.Date;

/**
 * 相位闪烁指数实体类
 * 对应TDengine表 ieims.phase_scintillation
 *
 * @author guet_developer01
 * @date 2026-05-11
 */
public class PhaseScintillation implements Serializable {

    private static final long serialVersionUID = 1L;

    /** 时间戳(毫秒) */
    private Long ts;

    /** 站点ID (TAG) */
    private String stationId;

    /** 卫星编号 (TAG) */
    private String satNo;

    /** σφ相位闪烁指数(0.0-10.0) */
    private Double phi4;

    /** 原始SigmaPhi值 */
    private Double rawSigmaPhi;

    /** 样本数量 */
    private Integer sampleCount;

    /** 闪烁等级(0:无闪烁,1:弱闪烁,2:中等闪烁,3:强闪烁) */
    private Integer levelCode;

    /** 等级名称 */
    private String levelName;

    /** 计算时间(毫秒) */
    private Long calcTime;

    /** 计算窗口开始时间(毫秒) */
    private Long windowStartTime;

    /** 计算窗口结束时间(毫秒) */
    private Long windowEndTime;

    // getter/setter
    public Long getTs() {
        return ts;
    }

    public void setTs(Long ts) {
        this.ts = ts;
    }

    public String getStationId() {
        return stationId;
    }

    public void setStationId(String stationId) {
        this.stationId = stationId;
    }

    public String getSatNo() {
        return satNo;
    }

    public void setSatNo(String satNo) {
        this.satNo = satNo;
    }

    public Double getPhi4() {
        return phi4;
    }

    public void setPhi4(Double phi4) {
        this.phi4 = phi4;
    }

    public Double getRawSigmaPhi() {
        return rawSigmaPhi;
    }

    public void setRawSigmaPhi(Double rawSigmaPhi) {
        this.rawSigmaPhi = rawSigmaPhi;
    }

    public Integer getSampleCount() {
        return sampleCount;
    }

    public void setSampleCount(Integer sampleCount) {
        this.sampleCount = sampleCount;
    }

    public Integer getLevelCode() {
        return levelCode;
    }

    public void setLevelCode(Integer levelCode) {
        this.levelCode = levelCode;
    }

    public String getLevelName() {
        return levelName;
    }

    public void setLevelName(String levelName) {
        this.levelName = levelName;
    }

    public Long getCalcTime() {
        return calcTime;
    }

    public void setCalcTime(Long calcTime) {
        this.calcTime = calcTime;
    }

    public Long getWindowStartTime() {
        return windowStartTime;
    }

    public void setWindowStartTime(Long windowStartTime) {
        this.windowStartTime = windowStartTime;
    }

    public Long getWindowEndTime() {
        return windowEndTime;
    }

    public void setWindowEndTime(Long windowEndTime) {
        this.windowEndTime = windowEndTime;
    }
}