package com.ruoyi.gnss.service.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * 卫星观测数据融合服务 (Caffeine TTL 安全版)
 *
 * 功能说明：
 * 1. 缓存 GSV 数据（仰角、方位角、信噪比），支持 5秒自动过期，防止内存泄漏。
 * 2. 缓存 RTCM 解算数据（伪距、相位），支持 5秒自动过期。
 * 3. 按"历元时间+卫星编号"融合数据
 * 4. 批量写入 TDengine
 *
 * @author GNSS Team
 */
@Service
public class SatelliteDataFusionService {

    private static final Logger logger = LoggerFactory.getLogger(SatelliteDataFusionService.class);

    // ==================== 缓存键分隔符 ====================
    private static final String KEY_SEPARATOR = ":";

    // ==================== 数据缓存 (支持多站点，引入 TTL) ====================

    // 【核心修复】引入 Caffeine 替代 ConcurrentHashMap，利用 5 秒 TTL 自动淘汰陈旧数据
    // 彻底解决强制清空导致的异步时间差丢失数据问题
    private final Cache<String, GsvSatelliteData> gsvCache = Caffeine.newBuilder()
            .maximumSize(10000)
            .expireAfterWrite(5, TimeUnit.SECONDS)
            .build();

    private final Cache<String, RtcmObsData> rtcmCache = Caffeine.newBuilder()
            .maximumSize(10000)
            .expireAfterWrite(5, TimeUnit.SECONDS)
            .build();

    private final ConcurrentHashMap<String, AtomicLong> stationLastFusionTime = new ConcurrentHashMap<>();

    // 隔离存放各个站点的当前历元时间
    private final ConcurrentHashMap<String, Long> stationEpochTimes = new ConcurrentHashMap<>();

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

    @PostConstruct
    public void init() {
        logger.info("卫星数据融合服务初始化完成（Caffeine TTL 安全版），融合间隔: {}ms, 最大缓存: {}", fusionIntervalMs, cacheMaxSize);
    }

    // ==================== 复合键生成 ====================

    private String buildCacheKey(String stationId, String satNo) {
        return stationId + KEY_SEPARATOR + satNo;
    }

    private String extractSatNo(String key) {
        int idx = key.indexOf(KEY_SEPARATOR);
        return idx > 0 ? key.substring(idx + 1) : key;
    }

    // ==================== 数据处理方法 ====================

    public void processGsvData(String gsvLine) {
        processGsvData(StationContext.getCurrentStationId(), gsvLine);
    }

    public void processGsvData(String stationId, String gsvLine) {
        if (gsvParser == null || gsvLine == null) {
            return;
        }
        try {
            List<GsvSatelliteData> satList = gsvParser.parseGsv(gsvLine);
            for (GsvSatelliteData data : satList) {
                if (data.getSatNo() != null) {
                    // Caffeine 自动维护 max size 和 expire，无需手动校验
                    gsvCache.put(buildCacheKey(stationId, data.getSatNo()), data);
                }
            }
        } catch (Exception e) {
            logger.error("处理 GSV 数据异常: {}", e.getMessage());
        }
    }

    public void processRtcmData(String satId, int rtcmMessageType,
                                Double p1, Double p2, Double l1, Double l2,
                                Double snr, String c1, String c2) {
        processRtcmData(StationContext.getCurrentStationId(), satId, rtcmMessageType,
                p1, p2, l1, l2, snr, c1, c2);
    }

    public void processRtcmData(String stationId, String satId, int rtcmMessageType,
                                Double p1, Double p2, Double l1, Double l2,
                                Double snr, String c1, String c2) {
        if (satId == null) {
            return;
        }
        try {
            String satNo = SatNoConverter.fromRtcmSatId(satId, rtcmMessageType);
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

            // Caffeine 自动维护，直接 Put
            rtcmCache.put(buildCacheKey(stationId, satNo), data);

        } catch (Exception e) {
            logger.error("处理 RTCM 数据异常: {}", e.getMessage());
        }
    }

    /**
     * 设置当前历元时间（多站点隔离）
     */
    public void setEpochTime(String stationId, Long epochTime) {
        if (stationId != null && epochTime != null) {
            stationEpochTimes.put(stationId, epochTime);
        }
    }

    /**
     * 兼容老代码的入口，默认使用 ThreadLocal 中的 stationId
     */
    public void setEpochTime(Long epochTime) {
        setEpochTime(StationContext.getCurrentStationId(), epochTime);
    }

    // ==================== 数据融合与入库 ====================

    public int fuseAndStore() {
        return fuseAndStore(StationContext.getCurrentStationId());
    }

    public int fuseAndStore(String stationId) {
        if (storageService == null || !storageService.isInitialized()) {
            return 0; // 删除了清空缓存的逻辑，交由 TTL 管理
        }

        long now = System.currentTimeMillis();

        AtomicLong lastFusionTime = stationLastFusionTime.computeIfAbsent(
                stationId, k -> new AtomicLong(0));

        long lastTime = lastFusionTime.get();
        if (now - lastTime < fusionIntervalMs) {
            return 0;
        }

        if (!lastFusionTime.compareAndSet(lastTime, now)) {
            return 0;
        }

        try {
            List<SatObservation> observations = fuseData(stationId, now);

            if (observations.isEmpty()) {
                return 0;
            }

            int savedCount = storageService.saveSatObservationBatch(stationId, observations);

            // =================================================================
            // 【核心修复点】不再调用 clearCacheForStation(stationId);
            // 将陈旧数据的清理彻底交给 Caffeine 的 5秒 TTL 控制。
            // 如此便可保证只有 RTCM 还没收到 GSV 的卫星平稳等待下一个包到达，防止被误删！
            // =================================================================

            return savedCount;

        } catch (Exception e) {
            logger.error("数据融合入库异常: {}", e.getMessage());
            return 0;
        }
    }

    private List<SatObservation> fuseData(String stationId, long timestamp) {
        Set<String> stationSatNos = new HashSet<>();
        String prefix = stationId + KEY_SEPARATOR;

        // 适配 Caffeine Map 获取方式
        for (String key : gsvCache.asMap().keySet()) {
            if (key.startsWith(prefix)) {
                stationSatNos.add(extractSatNo(key));
            }
        }
        for (String key : rtcmCache.asMap().keySet()) {
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

    private SatObservation fuseSatelliteData(String stationId, String satNo, long timestamp) {
        String key = buildCacheKey(stationId, satNo);
        // 适配 Caffeine 获取单个值
        GsvSatelliteData gsvData = gsvCache.getIfPresent(key);
        RtcmObsData rtcmData = rtcmCache.getIfPresent(key);

        if (gsvData == null && rtcmData == null) {
            return null;
        }

        SatObservation obs = new SatObservation();
        obs.setTimestamp(timestamp);

        // 从映射表中取得对应站点的历元时间
        obs.setEpochTime(stationEpochTimes.get(stationId));

        obs.setSatNo(satNo);
        obs.setSatSystem(SatNoConverter.getSatSystem(satNo));

        if (gsvData != null) {
            obs.setElevation(gsvData.getElevation());
            obs.setAzimuth(gsvData.getAzimuth());
            obs.setSnr(gsvData.getSnr());
        }

        if (rtcmData != null) {
            obs.setPseudorangeP1(rtcmData.getPseudorangeP1());
            obs.setPhaseL1(rtcmData.getPhaseL1());
            obs.setPseudorangeP2(rtcmData.getPseudorangeP2());
            obs.setPhaseP2(rtcmData.getPhaseL2());
            obs.setC1(rtcmData.getC1());
            obs.setC2(rtcmData.getC2());

            if (rtcmData.getSnr() != null && rtcmData.getSnr() > 0) {
                obs.setSnr(rtcmData.getSnr());
            }
        }

        obs.setDataSource(determineDataSource(gsvData, rtcmData));

        return obs;
    }

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

    public void clearCacheForStation(String stationId) {
        String prefix = stationId + KEY_SEPARATOR;
        gsvCache.asMap().keySet().removeIf(key -> key.startsWith(prefix));
        rtcmCache.asMap().keySet().removeIf(key -> key.startsWith(prefix));
        // 对于长时间不连的基站可以清理一下历元时间，防止小量泄露
        // stationEpochTimes.remove(stationId);
    }

    public void clearCache() {
        gsvCache.invalidateAll();
        rtcmCache.invalidateAll();
        stationEpochTimes.clear();
    }

    public int getGsvCacheSize() {
        return (int) gsvCache.estimatedSize();
    }

    public int getRtcmCacheSize() {
        return (int) rtcmCache.estimatedSize();
    }

    public String getCacheStatistics() {
        return String.format("Cache[GSV=%d, RTCM=%d, Stations=%d]",
                getGsvCacheSize(), getRtcmCacheSize(), stationLastFusionTime.size());
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