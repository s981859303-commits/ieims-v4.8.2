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

    @Value("${gnss.tdengine.database:ieims}")
    private String database;

    @Value("${gnss.parser.stationId:8900_1}")
    private String stationId;

    @Resource
    private TDengineUtil tdengineUtil;

    private boolean initialized = false;

    @PostConstruct
    public void init() {
        try {
            initTables();
            initialized = true;
            logger.info("TDengine 卫星观测数据存储服务初始化成功");
        } catch (Exception e) {
            logger.error("TDengine 卫星观测数据存储服务初始化失败: {}", e.getMessage());
        }
    }

    /**
     * 初始化超级表
     */
    private void initTables() {
        // 切换到目标数据库
        tdengineUtil.executeDDL("USE " + database);

        // 创建卫星观测数据超级表
        String createStableSql = String.format(
                "CREATE STABLE IF NOT EXISTS %s (" +
                        "ts TIMESTAMP, " +                    // 系统时间戳
                        "epoch_time TIMESTAMP, " +            // 历元时间
                        "sat_no VARCHAR(8), " +               // 卫星编号（G01/C01）
                        "sat_system VARCHAR(4), " +           // 卫星系统（GPS/BDS）
                        "elevation DOUBLE, " +                // 仰角（度）
                        "azimuth DOUBLE, " +                  // 方位角（度）
                        "snr DOUBLE, " +                      // 信噪比（dB-Hz）
                        "pseudorange_p1 DOUBLE, " +           // 伪距P1（米）
                        "phase_l1 DOUBLE, " +                 // 相位L1（周）
                        "pseudorange_p2 DOUBLE, " +           // 伪距P2（米）
                        "phase_p2 DOUBLE, " +                 // 相位P2（周）
                        "c1 VARCHAR(10), " +                  // 信号代码1
                        "c2 VARCHAR(10), " +                  // 信号代码2
                        "data_source VARCHAR(20) " +          // 数据来源
                        ") TAGS (station_id VARCHAR(50))",    // 站点ID标签
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
            String tableName = getTableName(observation.getSatNo());
            long timestamp = observation.getTimestamp() != null ?
                    observation.getTimestamp() : System.currentTimeMillis();

            // 使用自动建表语法
            String insertSql = buildInsertSql(tableName, timestamp, observation);
            tdengineUtil.executeUpdate(insertSql);

            logger.debug("卫星观测数据已存储: {}", observation.getSatNo());
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
            StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ");
            String sanitizedStationId = sanitizeTableName(stationId);
            int successCount = 0;

            for (SatObservation obs : observations) {
                if (obs == null || obs.getSatNo() == null) {
                    continue;
                }

                String tableName = "satobs_" + sanitizedStationId + "_" + obs.getSatNo();
                long timestamp = obs.getTimestamp() != null ?
                        obs.getTimestamp() : System.currentTimeMillis();

                // 拼接自动建表及插入语句
                sqlBuilder.append(buildAutoCreateInsertSql(tableName, timestamp, obs, stationId));
                successCount++;
            }

            if (successCount > 0) {
                tdengineUtil.executeUpdate(sqlBuilder.toString());
                logger.debug("批量存储卫星观测数据成功: {} 条", successCount);
            }

            return successCount;

        } catch (Exception e) {
            logger.error("批量存储卫星观测数据失败: {}", e.getMessage());
            return 0;
        }
    }

    /**
     * 构建自动建表插入语句
     */
    private String buildAutoCreateInsertSql(String tableName, long timestamp,
                                            SatObservation obs, String stationId) {

        return String.format(
                "%s USING %s TAGS ('%s') VALUES (%d, %s, '%s', '%s', %s, %s, %s, %s, %s, %s, %s, '%s', '%s', '%s') ",
                tableName,
                STABLE_NAME,
                stationId,
                timestamp,
                formatTimestamp(obs.getEpochTime()),
                obs.getSatNo(),
                obs.getSatSystem() != null ? obs.getSatSystem() : "",
                formatDouble(obs.getElevation()),
                formatDouble(obs.getAzimuth()),
                formatDouble(obs.getSnr()),
                formatDouble(obs.getPseudorangeP1()),
                formatDouble(obs.getPhaseL1()),
                formatDouble(obs.getPseudorangeP2()),
                formatDouble(obs.getPhaseP2()),
                obs.getC1() != null ? obs.getC1() : "",
                obs.getC2() != null ? obs.getC2() : "",
                obs.getDataSource() != null ? obs.getDataSource() : "FUSED"
        );
    }

    /**
     * 构建普通插入语句
     */
    private String buildInsertSql(String tableName, long timestamp, SatObservation obs) {
        return String.format(
                "INSERT INTO %s USING %s TAGS ('%s') VALUES (%d, %s, '%s', '%s', %s, %s, %s, %s, %s, %s, %s, '%s', '%s', '%s')",
                tableName,
                STABLE_NAME,
                stationId,
                timestamp,
                formatTimestamp(obs.getEpochTime()),
                obs.getSatNo(),
                obs.getSatSystem() != null ? obs.getSatSystem() : "",
                formatDouble(obs.getElevation()),
                formatDouble(obs.getAzimuth()),
                formatDouble(obs.getSnr()),
                formatDouble(obs.getPseudorangeP1()),
                formatDouble(obs.getPhaseL1()),
                formatDouble(obs.getPseudorangeP2()),
                formatDouble(obs.getPhaseP2()),
                obs.getC1() != null ? obs.getC1() : "",
                obs.getC2() != null ? obs.getC2() : "",
                obs.getDataSource() != null ? obs.getDataSource() : "FUSED"
        );
    }

    /**
     * 获取子表名称
     */
    private String getTableName(String satNo) {
        return "satobs_" + sanitizeTableName(stationId) + "_" + satNo;
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

    /**
     * 格式化时间戳
     */
    private String formatTimestamp(Long timestamp) {
        if (timestamp == null) {
            return "NULL";
        }
        return String.valueOf(timestamp);
    }

    /**
     * 格式化 Double 值
     */
    private String formatDouble(Double value) {
        if (value == null) {
            return "NULL";
        }
        return String.valueOf(value);
    }
}
