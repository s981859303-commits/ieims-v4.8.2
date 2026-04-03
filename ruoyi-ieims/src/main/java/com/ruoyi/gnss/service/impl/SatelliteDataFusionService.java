package com.ruoyi.gnss.service.impl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.ruoyi.gnss.domain.GsvSatelliteData;
import com.ruoyi.gnss.domain.SatObservation;
import com.ruoyi.gnss.service.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

/**
 * 卫星数据融合服务（修复版）
 *
 * 功能：
 * 1. 缓存 GSV 数据（仰角、方位角、信噪比）
 * 2. 缓存 RTCM 数据（伪距、相位）
 * 3. 融合 GSV + RTCM 数据，生成完整观测记录
 * 4. 关联 ZDA 日期，生成完整时间戳
 * 5. 处理跨天、日期缺失等边界场景
 *
 * 数据融合策略：
 * - 以"历元时间 + 卫星编号 + 日期"为键进行融合
 * - GSV 数据有效期：60秒
 * - RTCM 数据有效期：60秒
 * - ZDA 日期缓存：按站点隔离
 *
 * @version 2.1 - 2026-04-02 修复严重bug
 */
@Service
public class SatelliteDataFusionService {

    private static final Logger logger = LoggerFactory.getLogger(SatelliteDataFusionService.class);

    // ==================== 配置参数 ====================

    @Value("${gnss.fusion.gsvCacheExpireSec:60}")
    private int gsvCacheExpireSec;

    @Value("${gnss.fusion.rtcmCacheExpireSec:60}")
    private int rtcmCacheExpireSec;

    @Value("${gnss.fusion.zdaCacheExpireSec:300}")
    private int zdaCacheExpireSec;

    @Value("${gnss.fusion.pendingFlushThreshold:100}")
    private int pendingFlushThreshold;

    @Value("${gnss.fusion.pendingFlushIntervalMs:5000}")
    private long pendingFlushIntervalMs;

    // ==================== 依赖注入 ====================

    @Autowired(required = false)
    private GnssAsyncProcessor asyncProcessor;

    @Autowired(required = false)
    private ISatObservationStorageService storageService;

    // ==================== 缓存定义 ====================

    /** GSV 数据缓存：key = stationId_satNo */
    private Cache<String, GsvSatelliteData> gsvCache;

    /** RTCM 数据缓存：key = stationId_satNo */
    private Cache<String, RtcmObsData> rtcmCache;

    /** ZDA 日期缓存：key = stationId */
    private Cache<String, ZdaDateCache> zdaDateCache;

    /** 待入库观测数据：key = stationId */
    private final ConcurrentHashMap<String, List<SatObservation>> pendingObservations = new ConcurrentHashMap<>();

    // ==================== 时间格式化器 ====================

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HHmmss.SSS");
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    // ==================== 初始化 ====================

    @PostConstruct
    public void init() {
        // 初始化 GSV 缓存
        gsvCache = Caffeine.newBuilder()
                .expireAfterWrite(gsvCacheExpireSec, TimeUnit.SECONDS)
                .maximumSize(10000)
                .build();

        // 初始化 RTCM 缓存
        rtcmCache = Caffeine.newBuilder()
                .expireAfterWrite(rtcmCacheExpireSec, TimeUnit.SECONDS)
                .maximumSize(10000)
                .build();

        // 初始化 ZDA 日期缓存
        zdaDateCache = Caffeine.newBuilder()
                .expireAfterWrite(zdaCacheExpireSec, TimeUnit.SECONDS)
                .maximumSize(100)
                .build();

        logger.info("卫星数据融合服务初始化完成 - GSV缓存过期: {}s, RTCM缓存过期: {}s, ZDA缓存过期: {}s",
                gsvCacheExpireSec, rtcmCacheExpireSec, zdaCacheExpireSec);
    }

    // ==================== GSV 数据处理 ====================

    /**
     * 缓存 GSV 数据
     *
     * @param stationId 站点ID
     * @param gsvData GSV 卫星数据
     */
    public void cacheGsvData(String stationId, GsvSatelliteData gsvData) {
        if (stationId == null || gsvData == null || gsvData.getSatNo() == null) {
            return;
        }

        String cacheKey = buildCacheKey(stationId, gsvData.getSatNo());
        gsvCache.put(cacheKey, gsvData);

        logger.debug("缓存GSV数据: stationId={}, satNo={}, elevation={}, azimuth={}, snr={}",
                stationId, gsvData.getSatNo(), gsvData.getElevation(), gsvData.getAzimuth(), gsvData.getSnr());

        // 尝试融合
        tryFuseData(stationId, gsvData.getSatNo(), gsvData.getEpochTime());
    }

    /**
     * 批量缓存 GSV 数据
     *
     * @param stationId 站点ID
     * @param gsvList GSV 数据列表
     * @param epochTime 历元时间（毫秒）
     */
    public void cacheGsvBatch(String stationId, List<GsvSatelliteData> gsvList, long epochTime) {
        if (stationId == null || gsvList == null || gsvList.isEmpty()) {
            return;
        }

        for (GsvSatelliteData gsv : gsvList) {
            gsv.setEpochTime(epochTime);
            cacheGsvData(stationId, gsv);
        }
    }

    // ==================== RTCM 数据处理 ====================

    /**
     * 缓存 RTCM 观测数据
     *
     * 【修复说明】
     * - BUG-9,10,11: 修复了 obs.sys, obs.prn, obs.SNR 字段访问错误
     *   JavaObs 类中实际字段为: sat (int), snr (float[], 小写), id (byte[])
     * - BUG-12: 修复了 obs.code[] 类型错误，code 是 byte[] 不是 String[]
     *   需要使用 new String(obs.code, offset, len, StandardCharsets.UTF_8) 转换
     *
     * @param stationId 站点ID
     * @param obs RTCM 观测数据（来自 RtklibNative.JavaObs）
     * @param rtcmMessageType RTCM 消息类型（1074=GPS, 1127=BeiDou）
     */
    public void cacheRtcmData(String stationId, RtklibNative.JavaObs obs, int rtcmMessageType) {
        if (stationId == null || obs == null) {
            return;
        }

        // 【修复】从 obs.id (byte[]) 获取卫星ID字符串
        String satId = null;
        if (obs.id != null) {
            int len = 0;
            for (int i = 0; i < obs.id.length && obs.id[i] != 0; i++) {
                len++;
            }
            if (len > 0) {
                satId = new String(obs.id, 0, len, StandardCharsets.UTF_8).trim();
            }
        }

        // 【修复】使用 SatNoConverter 转换卫星编号
        String satNo;
        if (satId != null && !satId.isEmpty()) {
            satNo = SatNoConverter.fromRtcmSatId(satId, rtcmMessageType);
        } else {
            // 如果没有卫星ID，使用 sat 字段和消息类型
            satNo = SatNoConverter.fromRtcmMessageType(rtcmMessageType, obs.sat);
        }

        if (satNo == null) {
            logger.warn("无法解析卫星编号: satId={}, sat={}, msgType={}", satId, obs.sat, rtcmMessageType);
            return;
        }

        // 构建 RTCM 观测数据
        RtcmObsData rtcmData = new RtcmObsData();
        rtcmData.satNo = satNo;
        rtcmData.satSystem = SatNoConverter.getSatSystem(satNo);

        // 【修复】obs.snr 是 float[] 类型（小写），不是 SNR
        if (obs.snr != null && obs.snr.length >= 2) {
            rtcmData.snr1 = (double) obs.snr[0];
            rtcmData.snr2 = (double) obs.snr[1];
        }

        // 伪距和相位
        if (obs.P != null && obs.P.length >= 2) {
            rtcmData.pseudorangeP1 = obs.P[0];
            rtcmData.pseudorangeP2 = obs.P[1];
        }
        if (obs.L != null && obs.L.length >= 2) {
            rtcmData.phaseL1 = obs.L[0];
            rtcmData.phaseP2 = obs.L[1];
        }

        // 【修复】obs.code 是 byte[] 类型，需要转换为 String
        if (obs.code != null && obs.code.length >= 16) {
            // code[0-7] 是第一个频点的代码
            int len1 = 0;
            for (int i = 0; i < 8 && obs.code[i] != 0; i++) {
                len1++;
            }
            if (len1 > 0) {
                rtcmData.code1 = new String(obs.code, 0, len1, StandardCharsets.UTF_8).trim();
            }

            // code[8-15] 是第二个频点的代码
            int len2 = 0;
            for (int i = 8; i < 16 && obs.code[i] != 0; i++) {
                len2++;
            }
            if (len2 > 0) {
                rtcmData.code2 = new String(obs.code, 8, len2, StandardCharsets.UTF_8).trim();
            }
        }

        // 缓存
        String cacheKey = buildCacheKey(stationId, satNo);
        rtcmCache.put(cacheKey, rtcmData);

        logger.debug("缓存RTCM数据: stationId={}, satNo={}, P1={}, L1={}, snr1={}",
                stationId, satNo, rtcmData.pseudorangeP1, rtcmData.phaseL1, rtcmData.snr1);

        // 尝试融合
        tryFuseData(stationId, satNo, 0);
    }

    /**
     * 批量缓存 RTCM 数据
     *
     * @param stationId 站点ID
     * @param obsArray RTCM 观测数据数组
     * @param count 有效数据数量
     * @param rtcmMessageType RTCM 消息类型
     */
    public void cacheRtcmBatch(String stationId, RtklibNative.JavaObs[] obsArray, int count, int rtcmMessageType) {
        if (stationId == null || obsArray == null || count <= 0) {
            return;
        }

        for (int i = 0; i < count && i < obsArray.length; i++) {
            if (obsArray[i] != null) {
                cacheRtcmData(stationId, obsArray[i], rtcmMessageType);
            }
        }
    }

    // ==================== ZDA 日期处理 ====================

    /**
     * 更新并缓存 ZDA 日期（供 MixedLogSplitter 调用）
     *
     * @param stationId 站点ID
     * @param date      ZDA 观测日期
     * @param time      ZDA 观测时间
     * @param timestamp 接收到该数据的时间戳
     */
    public void updateZdaDate(String stationId, LocalDate date, LocalTime time, long timestamp) {
        if (stationId == null || date == null) {
            return;
        }

        ZdaDateCache cache = new ZdaDateCache();
        cache.date = date;
        cache.time = time;
        cache.timestamp = timestamp;
        cache.source = "ZDA";

        zdaDateCache.put(stationId, cache);

        logger.debug("更新ZDA日期: stationId={}, date={}, time={}, timestamp={}",
                stationId, cache.date, cache.time, cache.timestamp);
    }
    public void cacheZdaDate(String stationId, ZonedDateTime zdaDateTime) {
        if (stationId == null || zdaDateTime == null) {
            return;
        }

        ZdaDateCache cache = new ZdaDateCache();
        cache.date = zdaDateTime.toLocalDate();
        cache.time = zdaDateTime.toLocalTime();
        cache.timestamp = zdaDateTime.toInstant().toEpochMilli();
        cache.source = "ZDA";

        zdaDateCache.put(stationId, cache);

        logger.debug("缓存ZDA日期: stationId={}, date={}, time={}",
                stationId, cache.date, cache.time);
    }

    /**
     * 获取站点的当前日期
     *
     * @param stationId 站点ID
     * @return 日期，如果没有则返回当前系统日期
     */
    public LocalDate getCurrentDate(String stationId) {
        ZdaDateCache cache = zdaDateCache.getIfPresent(stationId);
        if (cache != null && cache.date != null) {
            return cache.date;
        }
        return LocalDate.now(ZoneOffset.UTC);
    }

    /**
     * 获取站点的当前时间
     *
     * @param stationId 站点ID
     * @return 时间，如果没有则返回 null
     */
    public LocalTime getCurrentTime(String stationId) {
        ZdaDateCache cache = zdaDateCache.getIfPresent(stationId);
        if (cache != null) {
            return cache.time;
        }
        return null;
    }

    // ==================== 数据融合 ====================

    /**
     * 处理 GSV 数据并触发融合
     *
     * @param stationId  站点ID
     * @param satellites GSV 卫星数据列表
     * @param epochTime  历元时间（毫秒）
     * @param obsDate    观测日期
     * @param dateSource 日期来源
     */
    public void processGsvData(String stationId, List<GsvSatelliteData> satellites, long epochTime, LocalDate obsDate, String dateSource) {
        if (stationId == null || satellites == null || satellites.isEmpty()) {
            return;
        }
        // 调用现存的批量缓存方法，其内部会自动触发 tryFuseData 进行融合
        cacheGsvBatch(stationId, satellites, epochTime);
    }

    /**
     * 处理 RTCM 数据并触发融合
     *
     * @param stationId         站点ID
     * @param obsArray          RTCM 观测数据数组
     * @param realGnssEpochTime 真实历元时间（毫秒）
     * @param obsDate           观测日期
     * @param dateSource        日期来源
     */
    public void processRtcmData(String stationId, RtklibNative.JavaObs[] obsArray, long realGnssEpochTime, LocalDate obsDate, String dateSource) {
        if (stationId == null || obsArray == null || obsArray.length == 0) {
            return;
        }

        // 遍历数组并存入缓存
        for (RtklibNative.JavaObs obs : obsArray) {
            if (obs != null) {
                // 传 0 代表未知 RTCM 类型（混合数据），内部会自动根据 obs.id 字节流去解析卫星号
                cacheRtcmData(stationId, obs, 0);

                // 由于原有 cacheRtcmData 方法参数里没有包含历元时间，
                // 我们在这里从缓存里取出来，补充上真实的历元时间后再次触发融合
                String satId = null;
                if (obs.id != null) {
                    int len = 0;
                    for (int i = 0; i < obs.id.length && obs.id[i] != 0; i++) {
                        len++;
                    }
                    if (len > 0) {
                        satId = new String(obs.id, 0, len, StandardCharsets.UTF_8).trim();
                    }
                }

                String satNo = null;
                if (satId != null && !satId.isEmpty()) {
                    satNo = SatNoConverter.fromRtcmSatId(satId, 0);
                } else {
                    satNo = SatNoConverter.fromRtcmMessageType(0, obs.sat);
                }

                if (satNo != null) {
                    RtcmObsData rtcmData = rtcmCache.getIfPresent(buildCacheKey(stationId, satNo));
                    if (rtcmData != null) {
                        rtcmData.epochTime = realGnssEpochTime; // 补充真实历元时间
                        tryFuseData(stationId, satNo, realGnssEpochTime); // 携带精确时间重新触发融合
                    }
                }
            }
        }
    }

    /**
     * 尝试融合数据
     *
     * @param stationId 站点ID
     * @param satNo 卫星编号
     * @param epochTime 历元时间（毫秒）
     */
    private void tryFuseData(String stationId, String satNo, long epochTime) {
        String cacheKey = buildCacheKey(stationId, satNo);

        GsvSatelliteData gsv = gsvCache.getIfPresent(cacheKey);
        RtcmObsData rtcm = rtcmCache.getIfPresent(cacheKey);

        // 只有当 GSV 或 RTCM 其中之一存在时才创建观测记录
        if (gsv == null && rtcm == null) {
            return;
        }

        // 创建观测数据
        SatObservation obs = new SatObservation();
        obs.setStationId(stationId);
        obs.setSatNo(satNo);
        obs.setTimestamp(System.currentTimeMillis());

        // 设置历元时间
        if (epochTime > 0) {
            obs.setEpochTime(epochTime);
        } else if (gsv != null && gsv.getEpochTime() != null) {
            obs.setEpochTime(gsv.getEpochTime());
        } else if (rtcm != null) {
            obs.setEpochTime(rtcm.epochTime);
        }

        // 合并 GSV 数据
        if (gsv != null) {
            obs.setElevation(gsv.getElevation());
            obs.setAzimuth(gsv.getAzimuth());
            obs.setSnr(gsv.getSnr());
            obs.setSatSystem(gsv.getSatSystem());
        }

        // 合并 RTCM 数据
        if (rtcm != null) {
            obs.setPseudorangeP1(rtcm.pseudorangeP1);
            obs.setPhaseL1(rtcm.phaseL1);
            obs.setPseudorangeP2(rtcm.pseudorangeP2);
            obs.setPhaseP2(rtcm.phaseP2);
            obs.setC1(rtcm.code1);
            obs.setC2(rtcm.code2);
            if (obs.getSatSystem() == null) {
                obs.setSatSystem(rtcm.satSystem);
            }
            // 如果 GSV 没有信噪比，使用 RTCM 的
            if (obs.getSnr() == null && rtcm.snr1 != null) {
                obs.setSnr(rtcm.snr1);
            }
        }

        // 设置数据来源
        if (gsv != null && rtcm != null) {
            obs.setDataSource(SatObservation.SOURCE_FUSED);
        } else if (gsv != null) {
            obs.setDataSource(SatObservation.SOURCE_GSV);
        } else {
            obs.setDataSource(SatObservation.SOURCE_RTCM);
        }

        // 【修复】设置日期和时间 - 不再调用私有方法
        // 使用公共方法设置日期和时间
        LocalDate date = getCurrentDate(stationId);
        obs.setObservationDate(date, SatObservation.DATE_SOURCE_ZDA);

        // 如果有历元时间，解析为观测时间
        if (obs.getEpochTime() != null && obs.getEpochTime() > 0) {
            obs.setEpochTimeAndParseTime(obs.getEpochTime());
        }

        // 添加到待入库队列
        addToPending(stationId, obs);
    }

    /**
     * 添加到待入库队列
     */
    private void addToPending(String stationId, SatObservation obs) {
        pendingObservations.computeIfAbsent(stationId, k -> new CopyOnWriteArrayList<>()).add(obs);

        // 检查是否需要刷新
        int pendingCount = getPendingObservationsCount();
        if (pendingCount >= pendingFlushThreshold) {
            flushPending();
        }
    }

    // ==================== 数据入库 ====================

    /**
     * 刷新待入库数据
     *
     * 【修复说明】
     * - BUG-14: 修复了方法名错误 submitSatObservation -> submitSatObservations
     */
    public synchronized void flushPending() {
        if (pendingObservations.isEmpty()) {
            return;
        }

        // 取出所有待入库数据
        Map<String, List<SatObservation>> toFlush = new HashMap<>();
        for (Map.Entry<String, List<SatObservation>> entry : pendingObservations.entrySet()) {
            if (!entry.getValue().isEmpty()) {
                toFlush.put(entry.getKey(), new ArrayList<>(entry.getValue()));
                entry.getValue().clear();
            }
        }

        if (toFlush.isEmpty()) {
            return;
        }

        // 提交到异步处理器
        for (Map.Entry<String, List<SatObservation>> entry : toFlush.entrySet()) {
            String stationId = entry.getKey();
            List<SatObservation> observations = entry.getValue();

            // 去重
            List<SatObservation> uniqueList = deduplicate(observations);

            if (!uniqueList.isEmpty()) {
                if (asyncProcessor != null) {
                    // 【修复】方法名改为 submitSatObservations
                    asyncProcessor.submitSatObservations(stationId, uniqueList);
                } else if (storageService != null) {
                    storageService.saveSatObservationBatch(stationId, uniqueList);
                }
                logger.debug("刷新待入库数据: stationId={}, count={}", stationId, uniqueList.size());
            }
        }
    }

    /**
     * 去重
     */
    private List<SatObservation> deduplicate(List<SatObservation> observations) {
        Map<String, SatObservation> uniqueMap = new LinkedHashMap<>();
        for (SatObservation obs : observations) {
            String key = obs.getObsUniqueKey();
            if (key != null) {
                uniqueMap.put(key, obs);
            }
        }
        return new ArrayList<>(uniqueMap.values());
    }

    /**
     * 手动提交观测数据
     *
     * 【修复说明】
     * - BUG-14: 新增此方法供外部调用
     */
    public boolean submitSatObservationBatch(List<SatObservation> observations) {
        if (observations == null || observations.isEmpty()) {
            return false;
        }

        // 按站点分组
        Map<String, List<SatObservation>> byStation = new HashMap<>();
        for (SatObservation obs : observations) {
            String stationId = obs.getStationId() != null ? obs.getStationId() : "default";
            byStation.computeIfAbsent(stationId, k -> new ArrayList<>()).add(obs);
        }

        // 提交到异步处理器
        for (Map.Entry<String, List<SatObservation>> entry : byStation.entrySet()) {
            if (asyncProcessor != null) {
                asyncProcessor.submitSatObservations(entry.getKey(), entry.getValue());
            } else if (storageService != null) {
                // 直接存储
                storageService.saveSatObservationBatch(entry.getKey(), entry.getValue());
            }
        }

        return true;
    }

    /**
     * 获取统计信息
     */
    public String getStatistics() {
        return String.format("GSV缓存=%d, RTCM缓存=%d, ZDA日期缓存=%d, 待入库=%d",
                getGsvCacheSize(), getRtcmCacheSize(), getZdaDateCacheSize(), getPendingObservationsCount());
    }

    public long getGsvCacheSize() {
        return gsvCache.estimatedSize();
    }

    public long getRtcmCacheSize() {
        return rtcmCache.estimatedSize();
    }

    public long getZdaDateCacheSize() {
        return zdaDateCache.estimatedSize();
    }

    public int getPendingObservationsCount() {
        return pendingObservations.values().stream().mapToInt(List::size).sum();
    }

    // ==================== 辅助方法 ====================

    /**
     * 构建缓存键
     */
    private String buildCacheKey(String stationId, String satNo) {
        return stationId + "_" + satNo;
    }

    // ==================== 内部类 ====================

    /**
     * RTCM 观测数据
     */
    private static class RtcmObsData {
        String satNo;
        String satSystem;
        Double pseudorangeP1;
        Double phaseL1;
        Double snr1;
        Double pseudorangeP2;
        Double phaseP2;
        Double snr2;
        String code1;
        String code2;
        long epochTime;
    }

    /**
     * ZDA 日期缓存
     */
    private static class ZdaDateCache {
        LocalDate date;
        LocalTime time;
        long timestamp;
        String source;
    }
}
