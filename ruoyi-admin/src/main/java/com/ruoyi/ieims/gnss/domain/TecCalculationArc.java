package com.ruoyi.ieims.gnss.domain;

import java.util.ArrayList;
import java.util.List;

/**
 * TEC计算弧段（内存滑动窗口对象，不映射数据库）
 *
 * @author guet_developer01
 * @date 2026-05-12
 */
public class TecCalculationArc {

    private String stationId;
    private String satNo;
    private List<TecSatObs> observations = new ArrayList<>();
    private Long lastTimestamp;
    private Double lastMw;
    private Double lastGf;
    private Double mwMean;
    private int slipCount = 0;
    /** 测站纬度（°），创建弧段时注入 */
    private Double stationLat;
    /** 测站经度（°），创建弧段时注入 */
    private Double stationLon;

    public void addObservation(TecSatObs obs) {
        this.observations.add(obs);
    }

    public void evictOlderThan(int minutes) {
        if (observations.isEmpty()) {
            return;
        }
        long cutoff = System.currentTimeMillis() - minutes * 60 * 1000L;
        observations.removeIf(o -> o.getTs().getTime() < cutoff);
    }

    public void reset() {
        observations.clear();
        lastMw = null;
        lastGf = null;
        mwMean = null;
    }

    public int size() {
        return observations.size();
    }

    public boolean hasDataInLastMinutes(int minutes) {
        if (lastTimestamp == null) {
            return false;
        }
        return (System.currentTimeMillis() - lastTimestamp) <= minutes * 60 * 1000L;
    }

    // getter / setter
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

    public List<TecSatObs> getObservations() {
        return observations;
    }

    public void setObservations(List<TecSatObs> observations) {
        this.observations = observations;
    }

    public Long getLastTimestamp() {
        return lastTimestamp;
    }

    public void setLastTimestamp(Long lastTimestamp) {
        this.lastTimestamp = lastTimestamp;
    }

    public Double getLastMw() {
        return lastMw;
    }

    public void setLastMw(Double lastMw) {
        this.lastMw = lastMw;
    }

    public Double getLastGf() {
        return lastGf;
    }

    public void setLastGf(Double lastGf) {
        this.lastGf = lastGf;
    }

    public Double getMwMean() {
        return mwMean;
    }

    public void setMwMean(Double mwMean) {
        this.mwMean = mwMean;
    }

    public int getSlipCount() {
        return slipCount;
    }

    public void setSlipCount(int slipCount) {
        this.slipCount = slipCount;
    }

    public void incrementSlipCount() {
        this.slipCount++;
    }

    public Double getStationLat() {
        return stationLat;
    }
    public void setStationLat(Double stationLat) {
        this.stationLat = stationLat;
    }
    public Double getStationLon() {
        return stationLon;
    }
    public void setStationLon(Double stationLon) {
        this.stationLon = stationLon;
    }
}