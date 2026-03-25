package com.ruoyi.gnss.service.impl;

import com.ruoyi.gnss.domain.SatObservation;
import com.ruoyi.gnss.service.ISatObservationStorageService;
import com.ruoyi.user.comm.core.tdengine.TDengineUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;

/**
 * TDengine 卫星观测数据存储服务实现类
 *
 * 功能说明：
 * 1. 创建卫星观测数据超级表 st_sat_observation
 * 2. 支持单条和批量数据写入
 * 3. 使用 TDengine 自动建表语法简化子表管理
 *
 * 表结构设计：
 * - 超级表：st_sat_observation
 * - 字段：ts, epoch_time, sat_no, sat_system, elevation, azimuth, snr,
 *         pseudorange_p1, phase_l1, pseudorange_p2, phase_p2, c1, c2, data_source
 * - 标签：station_id
 *
 * @author GNSS Team
 * @date 2026-03-25
 */
@Service
@ConditionalOnProperty(name = "gnss.tdengine.enabled", havingValue = "true", matchIfMissing = false)
public class TDengineSatObservationServiceImpl implements ISatObservationStorageService {

    private static final Logger logger = LoggerFactory.getLogger(TDengineSatObservationServiceImpl.class);

    /** 超级表名称 */
    private static final String STABLE_NAME = "st_sat_observation";

    /** 空字符串常量 */
    private static final String EMPTY_STR = "";
    private static final String NULL_STR = "NULL";
    private static final String FUSED_STR = "FUSED";

    @Value("${gnss.tdengine.database:ieims}")
    private String database;

    @Value("${gnss.parser.stationId:8900_1}")
    private String stationId;

    @Resource
    private TDengineUtil tdengineUtil;

    /** 缓存的站点ID */
    private String cachedSanitizedStationId;

    /** 缓存的表名前缀 */
    private String cachedTablePrefix;

    private volatile boolean initialized = false;

    @PostConstruct
    public void init() {
        try {
            this.cachedSanitizedStationId = sanitizeTableName(stationId);
            this.cachedTablePrefix = "satobs_" + cachedSanitizedStationId + "_";

            initTables();
            initialized = true;
            logger.info("TDengine 卫星观测数据存储服务初始化成功");
        } catch (Exception e) {
            logger.error("TDengine 卫星观测数据存储服务初始化失败: {}", e.getMessage());
        }
    }

    private void initTables() {
        tdengineUtil.executeDDL("USE " + database);

        String createStableSql = String.format(
                "CREATE STABLE IF NOT EXISTS %s (" +
                        "ts TIMESTAMP, " +
                        "epoch_time TIMESTAMP, " +
                        "sat_no VARCHAR(8), " +
                        "sat_system VARCHAR(4), " +
                        "elevation DOUBLE, " +
                        "azimuth DOUBLE, " +
                        "snr DOUBLE, " +
                        "pseudorange_p1 DOUBLE, " +
                        "phase_l1 DOUBLE, " +
                        "pseudorange_p2 DOUBLE, " +
                        "phase_p2 DOUBLE, " +
                        "c1 VARCHAR(10), " +
                        "c2 VARCHAR(10), " +
                        "data_source VARCHAR(20) " +
                        ") TAGS (station_id VARCHAR(50))",
                STABLE_NAME
        );

        tdengineUtil.executeDDL(createStableSql);
        logger.info("创建/确认超级表完成: {}", STABLE_NAME);
    }

    @Override
    public boolean isInitialized() {
        return initialized;
    }

    @Override
    public boolean saveSatObservation(SatObservation observation) {
        if (!initialized || observation == null || observation.getSatNo() == null) {
            return false;
        }

        try {
            long timestamp = observation.getTimestamp() != null ?
                    observation.getTimestamp() : System.currentTimeMillis();

            StringBuilder sb = new StringBuilder(250);
            appendInsertSql(sb, observation, timestamp, stationId);
            tdengineUtil.executeUpdate(sb.toString());

            return true;

        } catch (Exception e) {
            logger.error("存储卫星观测数据失败 [卫星: {}]: {}",
                    observation.getSatNo(), e.getMessage());
            return false;
        }
    }

    @Override
    public int saveSatObservationBatch(List<SatObservation> observations) {
        return saveSatObservationBatch(stationId, observations);
    }

    @Override
    public int saveSatObservationBatch(String stationId, List<SatObservation> observations) {
        if (!initialized || observations == null || observations.isEmpty()) {
            return 0;
        }

        try {
            // 预估 SQL 长度：每条约 200 字节
            int estimatedLength = 20 + observations.size() * 200;
            StringBuilder sqlBuilder = new StringBuilder(estimatedLength);
            sqlBuilder.append("INSERT INTO ");

            int successCount = 0;
            long timestamp = System.currentTimeMillis();

            for (SatObservation obs : observations) {
                if (obs == null || obs.getSatNo() == null) {
                    continue;
                }

                appendInsertSql(sqlBuilder, obs, timestamp, stationId);
                successCount++;
            }

            if (successCount > 0) {
                tdengineUtil.executeUpdate(sqlBuilder.toString());
            }

            return successCount;

        } catch (Exception e) {
            logger.error("批量存储卫星观测数据失败: {}", e.getMessage());
            return 0;
        }
    }

    /**
     * 追加插入 SQL 到 StringBuilder
     *
     * 修复：使用 String.valueOf() 替代 String.format(%f)，避免 Locale 陷阱
     */
    private void appendInsertSql(StringBuilder sb, SatObservation obs,
                                 long timestamp, String stationId) {
        sb.append(cachedTablePrefix)
                .append(obs.getSatNo())
                .append(" USING ")
                .append(STABLE_NAME)
                .append(" TAGS ('")
                .append(stationId)
                .append("') VALUES (")
                .append(timestamp)
                .append(", ")
                .append(formatTimestamp(obs.getEpochTime()))
                .append(", '")
                .append(obs.getSatNo())
                .append("', '")
                .append(obs.getSatSystem() != null ? obs.getSatSystem() : EMPTY_STR)
                .append("', ")
                .append(formatDouble(obs.getElevation()))
                .append(", ")
                .append(formatDouble(obs.getAzimuth()))
                .append(", ")
                .append(formatDouble(obs.getSnr()))
                .append(", ")
                .append(formatDouble(obs.getPseudorangeP1()))
                .append(", ")
                .append(formatDouble(obs.getPhaseL1()))
                .append(", ")
                .append(formatDouble(obs.getPseudorangeP2()))
                .append(", ")
                .append(formatDouble(obs.getPhaseP2()))
                .append(", '")
                .append(obs.getC1() != null ? obs.getC1() : EMPTY_STR)
                .append("', '")
                .append(obs.getC2() != null ? obs.getC2() : EMPTY_STR)
                .append("', '")
                .append(obs.getDataSource() != null ? obs.getDataSource() : FUSED_STR)
                .append("') ");
    }

    private String sanitizeTableName(String name) {
        if (name == null) {
            return "unknown";
        }
        return name.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    /**
     * 格式化时间戳
     * 安全：String.valueOf(long) 不受 Locale 影响
     */
    private String formatTimestamp(Long timestamp) {
        return timestamp != null ? String.valueOf(timestamp) : NULL_STR;
    }

    /**
     * 格式化 Double 值
     * 安全：String.valueOf(double) 不受 Locale 影响
     */
    private String formatDouble(Double value) {
        return value != null ? String.valueOf(value) : NULL_STR;
    }
}
