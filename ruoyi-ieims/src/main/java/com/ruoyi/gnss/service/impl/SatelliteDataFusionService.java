package com.ruoyi.gnss.service.impl;

import com.ruoyi.gnss.domain.GsvSatelliteData;
import com.ruoyi.gnss.domain.SatObservation;
import com.ruoyi.gnss.service.GsvParser;
import com.ruoyi.gnss.service.ISatObservationStorageService;
import com.ruoyi.gnss.service.SatNoConverter;
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

    /** GSV 数据缓存（键：卫星编号，值：卫星数据） */
    private final ConcurrentHashMap<String, GsvSatelliteData> gsvCache = new ConcurrentHashMap<>(64);

    /** RTCM 观测数据缓存（键：卫星编号，值：观测数据） */
    private final ConcurrentHashMap<String, RtcmObsData> rtcmCache = new ConcurrentHashMap<>(64);

    /** 当前历元时间（毫秒） */
    private volatile Long currentEpochTime = null;

    /** 最后融合时间 */
    private final AtomicLong lastFusionTime = new AtomicLong(0);

    @Value("${gnss.fusion.intervalMs:1000}")
    private long fusionIntervalMs;

    @Value("${gnss.fusion.epochTimeoutMs:2000}")
    private long epochTimeoutMs;

    @Autowired(required = false)
    private GsvParser gsvParser;

    @Autowired(required = false)
    private ISatObservationStorageService storageService;

    @PostConstruct
    public void init() {
        logger.info("卫星数据融合服务初始化完成，融合间隔: {}ms", fusionIntervalMs);
    }

    /**
     * 处理 GSV 数据
     *
     * @param gsvLine GSV 语句
     */
    public void processGsvData(String gsvLine) {
        if (gsvParser == null || gsvLine == null) {
            return;
        }

        try {
            List<GsvSatelliteData> satList = gsvParser.parseGsv(gsvLine);
            for (GsvSatelliteData data : satList) {
                if (data.getSatNo() != null) {
                    gsvCache.put(data.getSatNo(), data);
                }
            }
        } catch (Exception e) {
            logger.error("处理 GSV 数据异常: {}", e.getMessage());
        }
    }

    /**
     * 处理 RTCM 解算数据
     *
     * @param satId 卫星ID
     * @param rtcmMessageType RTCM 消息类型
     * @param p1 伪距P1
     * @param p2 伪距P2
     * @param l1 相位L1
     * @param l2 相位L2
     * @param snr 信噪比
     * @param c1 信号代码1
     * @param c2 信号代码2
     */
    public void processRtcmData(String satId, int rtcmMessageType,
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

            rtcmCache.put(satNo, data);

        } catch (Exception e) {
            logger.error("处理 RTCM 数据异常: {}", e.getMessage());
        }
    }

    /**
     * 设置当前历元时间
     *
     * @param epochTime 历元时间（毫秒）
     */
    public void setEpochTime(Long epochTime) {
        this.currentEpochTime = epochTime;
    }

    /**
     * 触发数据融合和入库
     *
     * @return 融合并入库的卫星数量
     */
    public int fuseAndStore() {
        return fuseAndStore(null);
    }

    /**
     * 触发数据融合和入库（指定站点ID）
     *
     * @param stationId 站点ID
     * @return 融合并入库的卫星数量
     */
    public int fuseAndStore(String stationId) {
        if (storageService == null || !storageService.isInitialized()) {
            logger.warn("存储服务未初始化，跳过融合入库");
            clearCache();
            return 0;
        }

        long now = System.currentTimeMillis();

        // 使用 CAS 操作避免并发问题
        long lastTime = lastFusionTime.get();
        if (now - lastTime < fusionIntervalMs) {
            return 0;
        }

        // CAS 更新最后融合时间
        if (!lastFusionTime.compareAndSet(lastTime, now)) {
            return 0;
        }

        try {
            List<SatObservation> observations = fuseData(now);

            if (observations.isEmpty()) {
                return 0;
            }

            int savedCount;
            if (stationId != null) {
                savedCount = storageService.saveSatObservationBatch(stationId, observations);
            } else {
                savedCount = storageService.saveSatObservationBatch(observations);
            }

            // 清空缓存
            clearCache();

            return savedCount;

        } catch (Exception e) {
            logger.error("数据融合入库异常: {}", e.getMessage());
            return 0;
        }
    }

    /**
     * 执行数据融合
     *
     * @param timestamp 系统时间戳
     * @return 融合后的卫星观测数据列表
     */
    private List<SatObservation> fuseData(long timestamp) {
        // 预分配容量，避免扩容
        int estimatedSize = Math.max(gsvCache.size(), rtcmCache.size());
        List<SatObservation> result = new ArrayList<>(estimatedSize);

        // 使用 Set 收集所有卫星编号
        Set<String> allSatNos = new HashSet<>(estimatedSize);
        allSatNos.addAll(gsvCache.keySet());
        allSatNos.addAll(rtcmCache.keySet());

        for (String satNo : allSatNos) {
            SatObservation obs = fuseSatelliteData(satNo, timestamp);
            if (obs != null) {
                result.add(obs);
            }
        }

        return result;
    }

    /**
     * 融合单颗卫星数据
     */
    private SatObservation fuseSatelliteData(String satNo, long timestamp) {
        GsvSatelliteData gsvData = gsvCache.get(satNo);
        RtcmObsData rtcmData = rtcmCache.get(satNo);

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

    /**
     * 清空缓存
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
     * RTCM 观测数据内部类
     */
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
