package com.ruoyi.gnss.service.impl;

import com.ruoyi.user.comm.core.tdengine.TDengineUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RTCM 观测数据存储服务实现类（TDengine 版本）
 *
 * <p>
 * 将 RTCM 1074/1127 解算出的卫星观测数据存储到 TDengine 数据库的 st_sat_obs 表中。
 * 数据来源：RTCM 1074（GPS MSM4）、RTCM 1127（BeiDou MSM4）等观测值消息
 * </p>
 *
 * <p>
 * 表结构说明：
 * - 超级表：st_sat_obs
 * - 字段：ts、elevation、azimuth、c1、snr1、p1、l1、c2、snr2、p2、l2
 * - 标签：station_id、sat_name
 * </p>
 *
 * @author GNSS Team
 * @date 2026-03-23
 */
@Service
@ConditionalOnProperty(name = "gnss.tdengine.enabled", havingValue = "true", matchIfMissing = false)
public class TDengineRtcmStorageServiceImpl {

    private static final Logger logger = LoggerFactory.getLogger(TDengineRtcmStorageServiceImpl.class);

    @Value("${gnss.tdengine.database:ieims}")
    private String database;

    @Value("${gnss.parser.stationId:8900_1}")
    private String stationId;

    private static final String STABLE_RTCM_RAW = "st_rtcm_raw";
    private static final String STABLE_SAT_OBS = "st_satellite_obs";

    @Resource
    private TDengineUtil tdengineUtil;

    private boolean initialized = false;

    @PostConstruct
    public void init() {
        try {
            initTables();
            initialized = true;
            logger.info("TDengine RTCM 存储服务初始化成功");
        } catch (Exception e) {
            logger.error("TDengine RTCM 存储服务初始化失败: {}", e.getMessage());
        }
    }

    private void initTables() {
        tdengineUtil.executeDDL("USE " + database);

        // RTCM 原始数据表
        String createRtcmStableSql = String.format(
                "CREATE STABLE IF NOT EXISTS %s (" +
                        "ts TIMESTAMP, data_len INT, data_base64 NCHAR(4096)" +
                        ") TAGS (station_id VARCHAR(50))",
                STABLE_RTCM_RAW
        );
        tdengineUtil.executeDDL(createRtcmStableSql);

        // 卫星观测数据表
        String createSatObsStableSql = String.format(
                "CREATE STABLE IF NOT EXISTS %s (" +
                        "ts TIMESTAMP, sat_name VARCHAR(10), c1 VARCHAR(10), c2 VARCHAR(10), " +
                        "p1 DOUBLE, p2 DOUBLE, l1 DOUBLE, l2 DOUBLE, s1 DOUBLE" +
                        ") TAGS (station_id VARCHAR(50))",
                STABLE_SAT_OBS
        );
        tdengineUtil.executeDDL(createSatObsStableSql);

        // 创建子表
        String rtcmTable = "rtcm_" + sanitizeTableName(stationId);
        String satObsTable = "satobs_" + sanitizeTableName(stationId);

        tdengineUtil.executeDDL(String.format(
                "CREATE TABLE IF NOT EXISTS %s USING %s TAGS ('%s')",
                rtcmTable, STABLE_RTCM_RAW, stationId
        ));

        tdengineUtil.executeDDL(String.format(
                "CREATE TABLE IF NOT EXISTS %s USING %s TAGS ('%s')",
                satObsTable, STABLE_SAT_OBS, stationId
        ));

        logger.info("创建 RTCM 表: {}, {}", rtcmTable, satObsTable);
    }

    public boolean saveRtcmRawData(byte[] rtcmData) {
        if (!initialized || rtcmData == null || rtcmData.length == 0) return false;

        try {
            String tableName = "rtcm_" + sanitizeTableName(stationId);
            long timestamp = System.currentTimeMillis();
            String base64Data = Base64.getEncoder().encodeToString(rtcmData);

            String insertSql = String.format(
                    "INSERT INTO %s (ts, data_len, data_base64) VALUES (?, ?, ?)",
                    tableName
            );

            tdengineUtil.executeUpdate(insertSql, timestamp, rtcmData.length, base64Data);
            logger.debug("RTCM 原始数据已存储，长度: {}", rtcmData.length);
            return true;

        } catch (Exception e) {
            logger.error("存储 RTCM 原始数据失败: {}", e.getMessage());
            return false;
        }
    }

    public boolean saveSatelliteObs(long timestamp, String satName,
                                    String c1, String c2, Double p1, Double p2, Double l1, Double l2, Double s1) {
        if (!initialized || satName == null) return false;

        try {
            String tableName = "satobs_" + sanitizeTableName(stationId);

            String insertSql = String.format(
                    "INSERT INTO %s (ts, sat_name, c1, c2, p1, p2, l1, l2, s1) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    tableName
            );

            tdengineUtil.executeUpdate(insertSql,
                    timestamp, satName,
                    c1 != null ? c1 : "", c2 != null ? c2 : "",
                    p1 != null ? p1 : 0.0, p2 != null ? p2 : 0.0,
                    l1 != null ? l1 : 0.0, l2 != null ? l2 : 0.0,
                    s1 != null ? s1 : 0.0
            );

            logger.debug("卫星观测数据已存储: {}", satName);
            return true;

        } catch (Exception e) {
            logger.error("存储卫星观测数据失败: {}", e.getMessage());
            return false;
        }
    }

    private String sanitizeTableName(String name) {
        if (name == null) return "unknown";
        return name.replaceAll("[^a-zA-Z0-9_]", "_");
    }
}
