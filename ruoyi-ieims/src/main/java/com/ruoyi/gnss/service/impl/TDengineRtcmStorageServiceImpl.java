package com.ruoyi.gnss.service.impl;

import com.ruoyi.gnss.service.IRtcmStorageService;
import com.ruoyi.user.comm.core.tdengine.TDengineUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Base64;
import java.util.List;
import java.util.Locale;

/**
 * TDengine RTCM 存储服务实现
 *
 */
@Service
@ConditionalOnProperty(name = "gnss.tdengine.enabled", havingValue = "true", matchIfMissing = false)
public class TDengineRtcmStorageServiceImpl implements IRtcmStorageService {

    private static final Logger logger = LoggerFactory.getLogger(TDengineRtcmStorageServiceImpl.class);

    @Value("${gnss.tdengine.database:ieims}")
    private String database;

    @Value("${gnss.parser.stationId:8900_1}")
    private String stationId;

    private static final String STABLE_RTCM_RAW = "st_rtcm_raw";
    private static final String STABLE_SAT_OBS = "st_satellite_obs";

    @Resource
    private TDengineUtil tdengineUtil;

    // 服务状态标识
    private boolean initialized = false;

    public static class ObsData {
        public String satName, obsTime, c1, c2;
        public Double p1, p2, l1, l2, s1, elevation, azimuth;

        public ObsData(String satName, String obsTime, String c1, String c2, Double p1, Double p2, Double l1, Double l2, Double s1, Double elevation, Double azimuth) {
            this.satName = satName; this.obsTime = obsTime; this.c1 = c1; this.c2 = c2;
            this.p1 = p1; this.p2 = p2; this.l1 = l1; this.l2 = l2; this.s1 = s1;
            this.elevation = elevation; this.azimuth = azimuth;
        }
    }

    @PostConstruct
    public void init() {
        try {
            initTables();
            initialized = true;
            logger.info("✅ TDengine RTCM 存储服务初始化成功 (支持星空数据融合)");
        } catch (Exception e) {
            logger.error("❌ TDengine RTCM 存储服务初始化失败: {}", e.getMessage());
        }
    }

    private void initTables() {
        try {
            tdengineUtil.executeDDL("CREATE DATABASE IF NOT EXISTS " + database);
        } catch (Exception ignored) {}

        String createRtcmStableSql = String.format(
                "CREATE STABLE IF NOT EXISTS %s (" +
                        "ts TIMESTAMP, data_len INT, data_base64 NCHAR(4096)" +
                        ") TAGS (station_id VARCHAR(50))",
                STABLE_RTCM_RAW
        );
        tdengineUtil.executeDDL(createRtcmStableSql);

        String createSatObsStableSql = String.format(
                "CREATE STABLE IF NOT EXISTS %s (" +
                        "ts TIMESTAMP, obs_time VARCHAR(30), c1 VARCHAR(10), c2 VARCHAR(10), " +
                        "p1 DOUBLE, p2 DOUBLE, l1 DOUBLE, l2 DOUBLE, s1 DOUBLE, " +
                        "elevation DOUBLE, azimuth DOUBLE" +
                        ") TAGS (station_id VARCHAR(50), sat_name VARCHAR(10))",
                STABLE_SAT_OBS
        );
        tdengineUtil.executeDDL(createSatObsStableSql);

        String rtcmTable = "rtcm_" + sanitizeTableName(stationId);
        tdengineUtil.executeDDL(String.format(
                "CREATE TABLE IF NOT EXISTS %s USING %s TAGS ('%s')",
                rtcmTable, STABLE_RTCM_RAW, stationId
        ));
    }

    // ==========================================
    // 🔥 核心修复：实现接口要求的抽象方法
    // ==========================================
    @Override
    public boolean isInitialized() {
        return this.initialized;
    }

    @Override
    public boolean saveRtcmRawData(byte[] rtcmData) {
        if (!initialized || rtcmData == null || rtcmData.length == 0) return false;
        try {
            String tableName = "rtcm_" + sanitizeTableName(stationId);
            long timestamp = System.currentTimeMillis();
            String base64Data = Base64.getEncoder().encodeToString(rtcmData);
            String insertSql = String.format(
                    "INSERT INTO %s (ts, data_len, data_base64) VALUES (%d, %d, '%s')",
                    tableName, timestamp, rtcmData.length, base64Data
            );
            tdengineUtil.executeUpdate(insertSql);
            return true;
        } catch (Exception e) {
            return false;
        }
    }

    @Async("gnssThreadPool")
    public void saveSatelliteObsBatch(long timestamp, List<ObsData> obsList) {
        if (!initialized || obsList == null || obsList.isEmpty()) return;

        // 统一添加 500 条分片策略，避免长度越界
        int batchSize = 500;
        for (int i = 0; i < obsList.size(); i += batchSize) {
            int end = Math.min(i + batchSize, obsList.size());
            executeSubBatch(timestamp, obsList.subList(i, end));
        }
    }

    private void executeSubBatch(long timestamp, List<ObsData> subList) {
        try {
            StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ");
            String sanitizedStationId = sanitizeTableName(stationId);

            for (ObsData obs : subList) {
                String tableName = "satobs_" + sanitizedStationId + "_" + sanitizeTableName(obs.satName);
                sqlBuilder.append(String.format(Locale.US,
                        "%s USING %s TAGS ('%s', '%s') VALUES (%d, '%s', '%s', '%s', %f, %f, %f, %f, %f, %f, %f) ",
                        tableName, STABLE_SAT_OBS, stationId, obs.satName,
                        timestamp,
                        obs.obsTime != null ? obs.obsTime : "",
                        obs.c1 != null ? obs.c1 : "",
                        obs.c2 != null ? obs.c2 : "",
                        obs.p1 != null ? obs.p1 : 0.0,
                        obs.p2 != null ? obs.p2 : 0.0,
                        obs.l1 != null ? obs.l1 : 0.0,
                        obs.l2 != null ? obs.l2 : 0.0,
                        obs.s1 != null ? obs.s1 : 0.0,
                        obs.elevation != null ? obs.elevation : 0.0,
                        obs.azimuth != null ? obs.azimuth : 0.0
                ));
            }
            tdengineUtil.executeUpdate(sqlBuilder.toString());
        } catch (Exception e) {
            logger.error("❌ 批量存储卫星观测数据失败: {}", e.getMessage());
        }
    }

    private String sanitizeTableName(String name) {
        if (name == null) return "unknown";
        return name.replaceAll("[^a-zA-Z0-9_]", "_");
    }
}