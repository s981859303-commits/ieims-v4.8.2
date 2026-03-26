package com.ruoyi.gnss.service.impl;

import com.ruoyi.gnss.domain.GsvSatelliteData;
import com.ruoyi.gnss.domain.SatObservation;
import com.ruoyi.gnss.service.GsvParser;
import com.ruoyi.gnss.service.ISatObservationStorageService;
import com.ruoyi.gnss.service.SatNoConverter;
import com.ruoyi.gnss.service.StationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 卫星观测数据融合服务
 *
 * 功能说明：
 * 1. 缓存 GSV 数据（仰角、方位角、信噪比）
 * 2. 缓存 RTCM 解算数据（伪距、相位）
 * 3. 按"历元时间+卫星编号"融合数据
 * 4. 批量写入 TDengine
 *
 * 融合规则：
 * - 仰角、方位角：仅 GSV 提供
 * - 信噪比：RTCM 优先，GSV 回退
 * - 伪距、相位：仅 RTCM 提供
 *
 * @author GNSS Team
 * @date 2026-03-25
 */
@Service
public class SatelliteDataFusionService {

    private static final Logger logger = LoggerFactory.getLogger(SatelliteDataFusionService.class);

    // ==================== 缓存键分隔符 ====================

    private static final String KEY_SEPARATOR = ":";

    // ==================== 数据缓存（支持多站点） ====================

    /**
     * GSV 数据缓存
     * 键格式：stationId:satNo
     */
    private final ConcurrentHashMap<String, GsvSatelliteData> gsvCache = new ConcurrentHashMap<>(256);

    /**
     * RTCM 观测数据缓存
     * 键格式：stationId:satNo
     */
    private final ConcurrentHashMap<String, RtcmObsData> rtcmCache = new ConcurrentHashMap<>(256);

    /**
     * 站点最后融合时间
     */
    private final ConcurrentHashMap<String, AtomicLong> stationLastFusionTime = new ConcurrentHashMap<>();

    // ==================== 配置参数 ====================

    @Value("${gnss.fusion.intervalMs:1000}")
    private long fusionIntervalMs;

    @Value("${gnss.fusion.epochTimeoutMs:2000}")
    private long epochTimeoutMs;

    @Value("${gnss.fusion.cacheMaxSize:10000}")
    private int cacheMaxSize;

    // ==================== 依赖注入 ====================

    @Autowired(required = false)
    private GsvParser gsvParser;

    @Autowired(required = false)
    private ISatObservationStorageService storageService;

    // ==================== 当前历元时间 ====================

    private volatile Long currentEpochTime = null;

    @PostConstruct
    public void init() {
        logger.info("卫星数据融合服务初始化完成，融合间隔: {}ms, 最大缓存: {}", fusionIntervalMs, cacheMaxSize);
    }

    // ==================== 复合键生成 ====================

    /**
     * 生成缓存键
     *
     * @param stationId 站点ID
     * @param satNo     卫星编号
     * @return 复合键
     */
    private String buildCacheKey(String stationId, String satNo) {
        return stationId + KEY_SEPARATOR + satNo;
    }

    /**
     * 从缓存键解析站点ID
     */
    private String extractStationId(String key) {
        int idx = key.indexOf(KEY_SEPARATOR);
        return idx > 0 ? key.substring(0, idx) : StationContext.getDefaultStationId();
    }

    /**
     * 从缓存键解析卫星编号
     */
    private String extractSatNo(String key) {
        int idx = key.indexOf(KEY_SEPARATOR);
        return idx > 0 ? key.substring(idx + 1) : key;
    }

    // ==================== 数据处理方法 ====================

    /**
     * 处理 GSV 数据
     *
     * @param gsvLine GSV 语句
     */
    public void processGsvData(String gsvLine) {
        processGsvData(StationContext.getCurrentStationId(), gsvLine);
    }

    /**
     * 处理 GSV 数据（指定站点）
     *
     * @param stationId 站点ID
     * @param gsvLine   GSV 语句
     */
    public void processGsvData(String stationId, String gsvLine) {
        if (gsvParser == null || gsvLine == null) {
            return;
        }

        try {
            List<GsvSatelliteData> satList = gsvParser.parseGsv(gsvLine);
            for (GsvSatelliteData data : satList) {
                if (data.getSatNo() != null) {
                    String key = buildCacheKey(stationId, data.getSatNo());

                    // 检查缓存大小
                    if (gsvCache.size() >= cacheMaxSize) {
                        logger.warn("GSV 缓存已满，跳过新数据");
                        return;
                    }

                    gsvCache.put(key, data);
                }
            }
        } catch (Exception e) {
            logger.error("处理 GSV 数据异常: {}", e.getMessage());
        }
    }

    /**
     * 处理 RTCM 解算数据
     */
    public void processRtcmData(String satId, int rtcmMessageType,
                                Double p1, Double p2, Double l1, Double l2,
                                Double snr, String c1, String c2) {
        processRtcmData(StationContext.getCurrentStationId(), satId, rtcmMessageType,
                p1, p2, l1, l2, snr, c1, c2);
    }

    /**
     * 处理 RTCM 解算数据（指定站点）
     */
    public void processRtcmData(String stationId, String satId, int rtcmMessageType,
                                Double p1, Double p2, Double l1, Double l2,
                                Double snr, String c1, String c2) {
        if (satId == null) {
            return;
        }

        try {
            String satNo = SatNoConverter.fromRtcmSatId(satId, rtcmMessageType);
            String key = buildCacheKey(stationId, satNo);

            RtcmObsData data = new RtcmObsData();
            data.setSatNo(satNo);
            data.setSatSystem(SatNoConverter.getSatSystem(satNo));
            data.setPseudorangeP1(p1);
            data.setPseudorangeP2(p2);
            data.setPhaseL1(l1);
            data.setPhaseL2(l2);
            data.setSnr(snr);
            data.setC1(c1);
            data.setC2(c2);
            data.setTimestamp(System.currentTimeMillis());

            // 检查缓存大小
            if (rtcmCache.size() >= cacheMaxSize) {
                logger.warn("RTCM 缓存已满，跳过新数据");
                return;
            }

            rtcmCache.put(key, data);

        } catch (Exception e) {
            logger.error("处理 RTCM 数据异常: {}", e.getMessage());
        }
    }

    /**
     * 设置当前历元时间
     */
    public void setEpochTime(Long epochTime) {
        this.currentEpochTime = epochTime;
    }

    // ==================== 数据融合与入库 ====================

    /**
     * 触发数据融合和入库
     */
    public int fuseAndStore() {
        return fuseAndStore(StationContext.getCurrentStationId());
    }

    /**
     * 触发数据融合和入库（指定站点）
     */
    public int fuseAndStore(String stationId) {
        if (storageService == null || !storageService.isInitialized()) {
            logger.warn("存储服务未初始化，跳过融合入库");
            clearCacheForStation(stationId);
            return 0;
        }

        long now = System.currentTimeMillis();

        // 获取或创建站点的最后融合时间
        AtomicLong lastFusionTime = stationLastFusionTime.computeIfAbsent(
                stationId, k -> new AtomicLong(0));

        long lastTime = lastFusionTime.get();
        if (now - lastTime < fusionIntervalMs) {
            return 0;
        }

        // CAS 更新最后融合时间
        if (!lastFusionTime.compareAndSet(lastTime, now)) {
            return 0;
        }

        try {
            List<SatObservation> observations = fuseData(stationId, now);

            if (observations.isEmpty()) {
                return 0;
            }

            int savedCount = storageService.saveSatObservationBatch(stationId, observations);

            // 清空该站点的缓存
            clearCacheForStation(stationId);

            return savedCount;

        } catch (Exception e) {
            logger.error("数据融合入库异常: {}", e.getMessage());
            return 0;
        }
    }

    /**
     * 执行数据融合（指定站点）
     */
    private List<SatObservation> fuseData(String stationId, long timestamp) {
        // 收集该站点的所有卫星编号
        Set<String> stationSatNos = new HashSet<>();

        String prefix = stationId + KEY_SEPARATOR;
        for (String key : gsvCache.keySet()) {
            if (key.startsWith(prefix)) {
                stationSatNos.add(extractSatNo(key));
            }
        }
        for (String key : rtcmCache.keySet()) {
            if (key.startsWith(prefix)) {
                stationSatNos.add(extractSatNo(key));
            }
        }

        if (stationSatNos.isEmpty()) {
            return Collections.emptyList();
        }

        List<SatObservation> result = new ArrayList<>(stationSatNos.size());

        for (String satNo : stationSatNos) {
            SatObservation obs = fuseSatelliteData(stationId, satNo, timestamp);
            if (obs != null) {
                result.add(obs);
            }
        }

        return result;
    }

    /**
     * 融合单颗卫星数据
     */
    private SatObservation fuseSatelliteData(String stationId, String satNo, long timestamp) {
        String key = buildCacheKey(stationId, satNo);
        GsvSatelliteData gsvData = gsvCache.get(key);
        RtcmObsData rtcmData = rtcmCache.get(key);

        if (gsvData == null && rtcmData == null) {
            return null;
        }

        SatObservation obs = new SatObservation();
        obs.setTimestamp(timestamp);
        obs.setEpochTime(currentEpochTime);
        obs.setSatNo(satNo);
        obs.setSatSystem(SatNoConverter.getSatSystem(satNo));

        // 融合 GSV 数据
        if (gsvData != null) {
            obs.setElevation(gsvData.getElevation());
            obs.setAzimuth(gsvData.getAzimuth());
            obs.setSnr(gsvData.getSnr());
        }

        // 融合 RTCM 数据
        if (rtcmData != null) {
            obs.setPseudorangeP1(rtcmData.getPseudorangeP1());
            obs.setPhaseL1(rtcmData.getPhaseL1());
            obs.setPseudorangeP2(rtcmData.getPseudorangeP2());
            obs.setPhaseP2(rtcmData.getPhaseL2());
            obs.setC1(rtcmData.getC1());
            obs.setC2(rtcmData.getC2());

            // RTCM 信噪比优先
            if (rtcmData.getSnr() != null && rtcmData.getSnr() > 0) {
                obs.setSnr(rtcmData.getSnr());
            }
        }

        // 设置数据来源标识
        obs.setDataSource(determineDataSource(gsvData, rtcmData));

        return obs;
    }

    /**
     * 确定数据来源标识
     */
    private String determineDataSource(GsvSatelliteData gsvData, RtcmObsData rtcmData) {
        if (gsvData != null && rtcmData != null) {
            return "FUSED";
        } else if (gsvData != null) {
            return "GSV";
        } else if (rtcmData != null) {
            return "RTCM";
        }
        return "UNKNOWN";
    }

    // ==================== 缓存管理 ====================

    /**
     * 清空指定站点的缓存
     */
    public void clearCacheForStation(String stationId) {
        String prefix = stationId + KEY_SEPARATOR;

        gsvCache.keySet().removeIf(key -> key.startsWith(prefix));
        rtcmCache.keySet().removeIf(key -> key.startsWith(prefix));
    }

    /**
     * 清空所有缓存
     */
    public void clearCache() {
        gsvCache.clear();
        rtcmCache.clear();
    }

    /**
     * 获取 GSV 缓存大小
     */
    public int getGsvCacheSize() {
        return gsvCache.size();
    }

    /**
     * 获取 RTCM 缓存大小
     */
    public int getRtcmCacheSize() {
        return rtcmCache.size();
    }

    /**
     * 获取缓存统计信息
     */
    public String getCacheStatistics() {
        return String.format("Cache[GSV=%d, RTCM=%d, Stations=%d]",
                gsvCache.size(), rtcmCache.size(), stationLastFusionTime.size());
    }

    // ==================== RTCM 观测数据内部类 ====================

    public static class RtcmObsData {
        private String satNo;
        private String satSystem;
        private Double pseudorangeP1;
        private Double pseudorangeP2;
        private Double phaseL1;
        private Double phaseL2;
        private Double snr;
        private String c1;
        private String c2;
        private Long timestamp;

        public String getSatNo() { return satNo; }
        public void setSatNo(String satNo) { this.satNo = satNo; }
        public String getSatSystem() { return satSystem; }
        public void setSatSystem(String satSystem) { this.satSystem = satSystem; }
        public Double getPseudorangeP1() { return pseudorangeP1; }
        public void setPseudorangeP1(Double pseudorangeP1) { this.pseudorangeP1 = pseudorangeP1; }
        public Double getPseudorangeP2() { return pseudorangeP2; }
        public void setPseudorangeP2(Double pseudorangeP2) { this.pseudorangeP2 = pseudorangeP2; }
        public Double getPhaseL1() { return phaseL1; }
        public void setPhaseL1(Double phaseL1) { this.phaseL1 = phaseL1; }
        public Double getPhaseL2() { return phaseL2; }
        public void setPhaseL2(Double phaseL2) { this.phaseL2 = phaseL2; }
        public Double getSnr() { return snr; }
        public void setSnr(Double snr) { this.snr = snr; }
        public String getC1() { return c1; }
        public void setC1(String c1) { this.c1 = c1; }
        public String getC2() { return c2; }
        public void setC2(String c2) { this.c2 = c2; }
        public Long getTimestamp() { return timestamp; }
        public void setTimestamp(Long timestamp) { this.timestamp = timestamp; }
    }
}
