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
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.*;

/**
 * 卫星数据融合服务（
 *
 * 功能：
 * 1. 缓存 GSV 数据（仰角、方位角、信噪比）
 * 2. 缓存 RTCM 数据（伪距、相位）
 * 3. 融合 GSV + RTCM 数据，生成完整观测记录
 * 4. 【新增】关联 ZDA 日期，生成完整时间戳
 * 5. 【新增】处理跨天、日期缺失等边界场景
 *
 * 数据融合策略：
 * - 以"历元时间 + 卫星编号 + 日期"为键进行融合
 * - GSV 数据有效期：60秒
 * - RTCM 数据有效期：60秒
 * - ZDA 日期缓存：按站点隔离
 *
 * @version 2.0 - 2026-04-02 添加ZDA日期关联支持
 */
@Service
public class SatelliteDataFusionService {

    private static final Logger logger = LoggerFactory.getLogger(SatelliteDataFusionService.class);

    // ==================== 常量定义 ====================

    /** GSV 数据缓存过期时间（秒） */
    private static final int GSV_CACHE_EXPIRE_SECONDS = 60;

    /** RTCM 数据缓存过期时间（秒） */
    private static final int RTCM_CACHE_EXPIRE_SECONDS = 60;

    /** ZDA 日期缓存过期时间（小时） */
    private static final int ZDA_DATE_CACHE_EXPIRE_HOURS = 24;

    /** 历元时间匹配容差（毫秒） */
    private static final long EPOCH_TIME_TOLERANCE_MS = 1000;

    /** 批量入库阈值 */
    private static final int BATCH_INSERT_THRESHOLD = 10;

    /** 批量入库超时（毫秒） */
    private static final long BATCH_INSERT_TIMEOUT_MS = 5000;

    // ==================== 依赖注入 ====================

    @Autowired(required = false)
    private GnssAsyncProcessor asyncProcessor;

    @Autowired(required = false)
    private ISatObservationStorageService storageService;

    @Value("${gnss.fusion.batchSize:100}")
    private int batchSize;

    @Value("${gnss.fusion.enabled:true}")
    private boolean fusionEnabled;

    // ==================== 缓存定义 ====================

    /** GSV 数据缓存：key = stationId_satNo, value = GsvSatelliteData */
    private Cache<String, GsvSatelliteData> gsvCache;

    /** RTCM 数据缓存：key = stationId_satNo, value = RtcmObsData */
    private Cache<String, RtcmObsData> rtcmCache;

    /** ZDA 日期缓存：key = stationId, value = ZdaDateCache */
    private Cache<String, ZdaDateCache> zdaDateCache;

    /** 待入库观测数据队列 */
    private final ConcurrentHashMap<String, List<SatObservation>> pendingObservations = new ConcurrentHashMap<>();

    /** 批量入库定时器 */
    private ScheduledExecutorService batchExecutor;

    // ==================== 初始化 ====================

    @PostConstruct
    public void init() {
        // 初始化 GSV 缓存
        gsvCache = Caffeine.newBuilder()
                .expireAfterWrite(GSV_CACHE_EXPIRE_SECONDS, TimeUnit.SECONDS)
                .maximumSize(10000)
                .recordStats()
                .build();

        // 初始化 RTCM 缓存
        rtcmCache = Caffeine.newBuilder()
                .expireAfterWrite(RTCM_CACHE_EXPIRE_SECONDS, TimeUnit.SECONDS)
                .maximumSize(10000)
                .recordStats()
                .build();

        // 初始化 ZDA 日期缓存
        zdaDateCache = Caffeine.newBuilder()
                .expireAfterWrite(ZDA_DATE_CACHE_EXPIRE_HOURS, TimeUnit.HOURS)
                .maximumSize(1000)
                .build();

        // 启动批量入库定时器
        batchExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "SatObs-Batch-Executor");
            t.setDaemon(true);
            return t;
        });
        batchExecutor.scheduleWithFixedDelay(this::flushPendingObservations,
                BATCH_INSERT_TIMEOUT_MS, BATCH_INSERT_TIMEOUT_MS, TimeUnit.MILLISECONDS);

        logger.info("SatelliteDataFusionService 初始化完成，批量大小: {}, 融合开关: {}",
                batchSize, fusionEnabled);
    }

    // ==================== 公共接口 ====================

    /**
     * 更新站点的 ZDA 日期
     *
     * @param stationId 站点ID
     * @param date      日期
     * @param time      时间
     * @param timestamp 时间戳
     */
    public void updateZdaDate(String stationId, LocalDate date, LocalTime time, long timestamp) {
        if (stationId == null || date == null) {
            return;
        }

        ZdaDateCache cache = zdaDateCache.get(stationId, k -> new ZdaDateCache());
        LocalDate oldDate = cache.date;

        cache.date = date;
        cache.time = time;
        cache.timestamp = timestamp;
        cache.source = "ZDA";

        if (oldDate != null && !oldDate.equals(date)) {
            logger.info("站点 {} ZDA日期变更: {} -> {}", stationId, oldDate, date);
        }

        logger.debug("站点 {} ZDA日期更新: {} {}", stationId, date, time);
    }

    /**
     * 处理 GSV 数据
     *
     * @param stationId 站点ID
     * @param satellites 卫星数据列表
     * @param epochTime  历元时间
     * @param obsDate    观测日期（来自ZDA）
     * @param dateSource 日期来源
     */
    public void processGsvData(String stationId, List<GsvSatelliteData> satellites,
                               long epochTime, LocalDate obsDate, String dateSource) {
        if (!fusionEnabled || satellites == null || satellites.isEmpty()) {
            return;
        }

        // 获取站点日期
        LocalDate effectiveDate = getEffectiveDate(stationId, obsDate, dateSource);

        for (GsvSatelliteData sat : satellites) {
            String cacheKey = buildCacheKey(stationId, sat.getSatNo());

            // 缓存 GSV 数据
            gsvCache.put(cacheKey, sat);

            // 尝试融合
            tryFuseAndSave(stationId, sat.getSatNo(), epochTime, effectiveDate, dateSource);
        }

        logger.debug("站点 {} 处理 GSV 数据: {} 颗卫星, 日期={}",
                stationId, satellites.size(), effectiveDate);
    }

    /**
     * 处理 RTCM 数据
     *
     * @param stationId 站点ID
     * @param obsArray  观测数据数组
     * @param epochTime 历元时间
     * @param obsDate   观测日期（来自ZDA）
     * @param dateSource 日期来源
     */
    public void processRtcmData(String stationId, RtklibNative.JavaObs[] obsArray,
                                Long epochTime, LocalDate obsDate, String dateSource) {
        if (!fusionEnabled || obsArray == null || obsArray.length == 0) {
            return;
        }

        // 获取站点日期
        LocalDate effectiveDate = getEffectiveDate(stationId, obsDate, dateSource);
        long effectiveEpochTime = epochTime != null ? epochTime : System.currentTimeMillis();

        for (RtklibNative.JavaObs obs : obsArray) {
            String satNo = SatNoConverter.convert(obs.sys, obs.prn);
            String cacheKey = buildCacheKey(stationId, satNo);

            // 缓存 RTCM 数据
            RtcmObsData rtcmData = new RtcmObsData();
            rtcmData.satNo = satNo;
            rtcmData.satSystem = getSatSystemName(obs.sys);
            rtcmData.pseudorangeP1 = obs.P[0];
            rtcmData.phaseL1 = obs.L[0];
            rtcmData.snr1 = obs.SNR[0];
            rtcmData.pseudorangeP2 = obs.P[1];
            rtcmData.phaseP2 = obs.L[1];
            rtcmData.snr2 = obs.SNR[1];
            rtcmData.code1 = obs.code[0];
            rtcmData.code2 = obs.code[1];
            rtcmData.epochTime = effectiveEpochTime;
            rtcmCache.put(cacheKey, rtcmData);

            // 尝试融合
            tryFuseAndSave(stationId, satNo, effectiveEpochTime, effectiveDate, dateSource);
        }

        logger.debug("站点 {} 处理 RTCM 数据: {} 个观测值, 日期={}",
                stationId, obsArray.length, effectiveDate);
    }

    /**
     * 尝试融合并保存数据
     */
    private void tryFuseAndSave(String stationId, String satNo, long epochTime,
                                LocalDate obsDate, String dateSource) {
        String cacheKey = buildCacheKey(stationId, satNo);

        // 获取 GSV 数据
        GsvSatelliteData gsvData = gsvCache.getIfPresent(cacheKey);

        // 获取 RTCM 数据
        RtcmObsData rtcmData = rtcmCache.getIfPresent(cacheKey);

        // 检查是否可以融合
        if (gsvData == null && rtcmData == null) {
            return;
        }

        // 检查历元时间是否匹配（如果两边都有）
        if (gsvData != null && rtcmData != null) {
            long gsvTime = gsvData.getEpochTime() != null ? gsvData.getEpochTime() : 0;
            long rtcmTime = rtcmData.epochTime;
            if (Math.abs(gsvTime - rtcmTime) > EPOCH_TIME_TOLERANCE_MS) {
                logger.debug("站点 {} 卫星 {} 历元时间不匹配: GSV={}, RTCM={}",
                        stationId, satNo, gsvTime, rtcmTime);
                // 使用较新的数据
                epochTime = Math.max(gsvTime, rtcmTime);
            }
        }

        // 构建融合后的观测数据
        SatObservation observation = buildObservation(
                stationId, satNo, epochTime, obsDate, dateSource, gsvData, rtcmData);

        if (observation != null) {
            addToPendingList(stationId, observation);
        }
    }

    /**
     * 构建观测数据对象
     */
    private SatObservation buildObservation(String stationId, String satNo, long epochTime,
                                            LocalDate obsDate, String dateSource,
                                            GsvSatelliteData gsvData, RtcmObsData rtcmData) {
        SatObservation obs = new SatObservation();

        // 设置基础信息
        obs.setTimestamp(System.currentTimeMillis());
        obs.setEpochTime(epochTime);
        obs.setSatNo(satNo);
        obs.setStationId(stationId);

        // 【关键】设置日期信息
        obs.setObservationDate(obsDate);
        obs.setObsTime(epochTimeToTime(epochTime));
        obs.setDateSource(dateSource);

        // 计算唯一键
        obs.calculateObsUniqueKey();

        // 计算完整时间戳
        obs.calculateFullTimestamp();

        // 设置 GSV 数据
        if (gsvData != null) {
            obs.setSatSystem(gsvData.getSatSystem());
            obs.setElevation(gsvData.getElevation());
            obs.setAzimuth(gsvData.getAzimuth());
            obs.setSnr(gsvData.getSnr());
        }

        // 设置 RTCM 数据
        if (rtcmData != null) {
            obs.setSatSystem(rtcmData.satSystem);
            obs.setPseudorangeP1(rtcmData.pseudorangeP1);
            obs.setPhaseL1(rtcmData.phaseL1);
            obs.setPseudorangeP2(rtcmData.pseudorangeP2);
            obs.setPhaseP2(rtcmData.phaseP2);
            obs.setC1(rtcmData.code1);
            obs.setC2(rtcmData.code2);
            // 如果 GSV 没有信噪比，使用 RTCM 的
            if (gsvData == null || gsvData.getSnr() == null) {
                obs.setSnr(rtcmData.snr1);
            }
        }

        // 设置数据来源标记
        String dataSource = "";
        if (gsvData != null) dataSource += "GSV";
        if (rtcmData != null) dataSource += (dataSource.isEmpty() ? "RTCM" : "+RTCM");
        obs.setDataSource(dataSource);

        return obs;
    }

    /**
     * 获取有效日期
     */
    private LocalDate getEffectiveDate(String stationId, LocalDate providedDate, String dateSource) {
        // 如果提供了日期，直接使用
        if (providedDate != null) {
            return providedDate;
        }

        // 从缓存获取
        ZdaDateCache cache = zdaDateCache.getIfPresent(stationId);
        if (cache != null && cache.date != null) {
            return cache.date;
        }

        // 使用系统日期
        LocalDate systemDate = LocalDate.now();
        logger.warn("站点 {} 未获取到ZDA日期，使用系统日期: {}", stationId, systemDate);
        return systemDate;
    }

    /**
     * 历元时间转换为 LocalTime
     */
    private LocalTime epochTimeToTime(long epochTimeMs) {
        // 历元时间是一天内的毫秒数
        long msInDay = epochTimeMs % (24 * 60 * 60 * 1000);
        return LocalTime.ofNanoOfDay(msInDay * 1_000_000);
    }

    /**
     * 添加到待入库列表
     */
    private void addToPendingList(String stationId, SatObservation observation) {
        List<SatObservation> list = pendingObservations.computeIfAbsent(
                stationId, k -> Collections.synchronizedList(new ArrayList<>()));

        list.add(observation);

        // 达到批量阈值时触发入库
        if (list.size() >= batchSize) {
            flushPendingObservations(stationId);
        }
    }

    /**
     * 刷新所有待入库数据
     */
    private void flushPendingObservations() {
        for (String stationId : pendingObservations.keySet()) {
            flushPendingObservations(stationId);
        }
    }

    /**
     * 刷新指定站点的待入库数据
     */
    private void flushPendingObservations(String stationId) {
        List<SatObservation> list = pendingObservations.get(stationId);
        if (list == null || list.isEmpty()) {
            return;
        }

        // 取出当前列表
        List<SatObservation> toSave;
        synchronized (list) {
            if (list.isEmpty()) {
                return;
            }
            toSave = new ArrayList<>(list);
            list.clear();
        }

        // 去重（按唯一键）
        Map<String, SatObservation> uniqueMap = new LinkedHashMap<>();
        for (SatObservation obs : toSave) {
            uniqueMap.put(obs.getObsUniqueKey(), obs);
        }
        List<SatObservation> uniqueList = new ArrayList<>(uniqueMap.values());

        // 提交入库
        if (asyncProcessor != null) {
            asyncProcessor.submitSatObservation(stationId, uniqueList);
        } else if (storageService != null) {
            storageService.saveSatObservationBatch(stationId, uniqueList);
        }

        logger.debug("站点 {} 刷新观测数据: {} 条（去重后 {} 条）",
                stationId, toSave.size(), uniqueList.size());
    }

    // ==================== 辅助方法 ====================

    /**
     * 构建缓存键
     */
    private String buildCacheKey(String stationId, String satNo) {
        return stationId + "_" + satNo;
    }

    /**
     * 获取卫星系统名称
     */
    private String getSatSystemName(int sys) {
        switch (sys) {
            case 0: return "GPS";
            case 1: return "GLONASS";
            case 2: return "GALILEO";
            case 3: return "QZSS";
            case 4: return "BDS";
            case 5: return "IRNSS";
            case 6: return "SBAS";
            default: return "UNKNOWN";
        }
    }

    // ==================== 统计接口 ====================

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

    public String getStatistics() {
        return String.format("GSV缓存=%d, RTCM缓存=%d, ZDA日期缓存=%d, 待入库=%d",
                getGsvCacheSize(), getRtcmCacheSize(), getZdaDateCacheSize(), getPendingObservationsCount());
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
