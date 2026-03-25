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

    /** RTCM 原始数据超级表 */
    private static final String STABLE_RTCM_RAW = "st_rtcm_raw";

    @Resource
    private TDengineUtil tdengineUtil;

    /** 缓存的站点ID（避免重复调用 sanitizeTableName） */
    private String cachedSanitizedStationId;

    /** 缓存的 RTCM 原始数据表名 */
    private String cachedRtcmTableName;

    private volatile boolean initialized = false;

    @PostConstruct
    public void init() {
        try {
            // 预先计算并缓存常用字符串
            this.cachedSanitizedStationId = sanitizeTableName(stationId);
            this.cachedRtcmTableName = "rtcm_" + cachedSanitizedStationId;

            initTables();
            initialized = true;
            logger.info("TDengine RTCM 存储服务初始化成功");
        } catch (Exception e) {
            logger.error("TDengine RTCM 存储服务初始化失败: {}", e.getMessage());
        }
    }

    /**
     * 初始化超级表
     *
     * 注意：st_satellite_obs 表已删除，由 TDengineSatObservationServiceImpl 统一管理
     */
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

        // 预建 RTCM raw 的子表
        tdengineUtil.executeDDL(String.format(
                "CREATE TABLE IF NOT EXISTS %s USING %s TAGS ('%s')",
                cachedRtcmTableName, STABLE_RTCM_RAW, stationId
        ));

        logger.info("创建/确认超级表完成: {}", STABLE_RTCM_RAW);
    }

    /**
     * 保存 RTCM 原始数据
     */
    public boolean saveRtcmRawData(byte[] rtcmData) {
        if (!initialized || rtcmData == null || rtcmData.length == 0) {
            return false;
        }

        try {
            long timestamp = System.currentTimeMillis();
            String base64Data = Base64.getEncoder().encodeToString(rtcmData);

            String insertSql = String.format(
                    "INSERT INTO %s (ts, data_len, data_base64) VALUES (?, ?, ?)",
                    cachedRtcmTableName
            );

            tdengineUtil.executeUpdate(insertSql, timestamp, rtcmData.length, base64Data);
            return true;

        } catch (Exception e) {
            logger.error("存储 RTCM 原始数据失败: {}", e.getMessage());
            return false;
        }
    }

    /**
     * 内部类：观测数据
     */
    public static class ObsData {
        public String satName, c1, c2;
        public Double p1, p2, l1, l2, s1;

        public ObsData(String satName, String c1, String c2,
                       Double p1, Double p2, Double l1, Double l2, Double s1) {
            this.satName = satName;
            this.c1 = c1;
            this.c2 = c2;
            this.p1 = p1;
            this.p2 = p2;
            this.l1 = l1;
            this.l2 = l2;
            this.s1 = s1;
        }
    }

    /**
     * 清理表名中的非法字符
     */
    private String sanitizeTableName(String name) {
        if (name == null) {
            return "unknown";
        }
        return name.replaceAll("[^a-zA-Z0-9_]", "_");
    }
}