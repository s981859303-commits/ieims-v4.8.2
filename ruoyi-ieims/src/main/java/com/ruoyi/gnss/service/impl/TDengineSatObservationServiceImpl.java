package com.ruoyi.gnss.service.impl;

import com.ruoyi.gnss.domain.SatObservation;
import com.ruoyi.gnss.service.ISatObservationStorageService;
import com.ruoyi.user.comm.core.tdengine.TDengineUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.LocalDate;
import java.util.Collections;
import java.util.List;
import java.util.Locale;

/**
 * TDengine 卫星观测数据 存储服务实现
 */
@Service
@ConditionalOnProperty(name = "gnss.tdengine.enabled", havingValue = "true", matchIfMissing = false)
public class TDengineSatObservationServiceImpl implements ISatObservationStorageService {

    private static final Logger log = LoggerFactory.getLogger(TDengineSatObservationServiceImpl.class);

    @Value("${gnss.tdengine.database:ieims}")
    private String database;

    private static final String STABLE_NAME = "st_sat_observation";

    @Resource
    private TDengineUtil tdengineUtil;

    private boolean initialized = false;

    @PostConstruct
    public void init() {
        try {
            initTables();
            initialized = true;
            log.info("✅ TDengine 卫星观测数据存储服务初始化成功");
        } catch (Exception e) {
            log.error("❌ TDengine 卫星观测表初始化失败: {}", e.getMessage());
            e.printStackTrace();
        }
    }

    private void initTables() {
        try {
            tdengineUtil.executeDDL("CREATE DATABASE IF NOT EXISTS " + database);
        } catch (Exception e) {
            log.debug("尝试建库失败 (云端版或已存在可忽略): {}", e.getMessage());
        }

        // ==========================================
        // 关键修复：先删除旧的、结构不匹配的超级表（清理脏数据）
        // ==========================================
        try {
            tdengineUtil.executeDDL("DROP STABLE IF EXISTS " + database + "." + STABLE_NAME);
            log.info("✅ 已清理旧的超级表，准备重建匹配的 TAGS 结构");
        } catch (Exception e) {
            log.debug("清理旧超级表失败忽略: {}", e.getMessage());
        }

        String createSuperTableSql = String.format(
                "CREATE STABLE IF NOT EXISTS %s.%s (" +
                        "ts TIMESTAMP, " +
                        "obs_time VARCHAR(30), " +
                        "sys_time BIGINT, " +
                        "c1 VARCHAR(10), c2 VARCHAR(10), c3 VARCHAR(10), " +
                        "p1 DOUBLE, p2 DOUBLE, p3 DOUBLE, " +
                        "l1 DOUBLE, l2 DOUBLE, l3 DOUBLE, " +
                        "s1 DOUBLE, s2 DOUBLE, s3 DOUBLE, " +
                        "elevation DOUBLE, azimuth DOUBLE" +
                        ") TAGS (station_id VARCHAR(50), sat_name VARCHAR(10))",
                database, STABLE_NAME
        );
        tdengineUtil.executeDDL(createSuperTableSql);
        log.info("✅ 创建/确认卫星观测超级表完成: {}.{}", database, STABLE_NAME);
    }

    @Override
    public void saveSatObservation(String stationId, SatObservation observation) {
        if (observation != null) {
            observation.setStationId(stationId);
            saveSatObservationBatch(Collections.singletonList(observation));
        }
    }

    @Override
    public void saveSatObservationBatch(String stationId, List<SatObservation> observations) {
        if (observations != null && !observations.isEmpty()) {
            observations.forEach(obs -> {
                if (obs.getStationId() == null || obs.getStationId().isEmpty()) {
                    obs.setStationId(stationId);
                }
            });
            saveSatObservationBatch(observations);
        }
    }

    @Override
    @Async("gnssThreadPool")
    public void saveSatObservationBatch(List<SatObservation> obsList) {
        if (!initialized || obsList == null || obsList.isEmpty()) {
            log.warn("服务未初始化成功或数据为空，跳过批量保存操作");
            return;
        }

        int batchSize = 500;
        for (int i = 0; i < obsList.size(); i += batchSize) {
            int end = Math.min(i + batchSize, obsList.size());
            executeBatchSql(obsList.subList(i, end));
        }
    }

    private void executeBatchSql(List<SatObservation> obsList) {
        try {
            StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ");

            // 按表名分组，并使用 LinkedHashMap<Long, SatObservation> 实现同批次同时间戳(ts)强去重
            java.util.Map<String, java.util.Map<Long, SatObservation>> tableGroup = new java.util.LinkedHashMap<>();
            for (SatObservation obs : obsList) {
                // 显式追加数据库名，防止找不到目标表
                String tableName = database + ".satobs_" + sanitizeTableName(obs.getStationId()) + "_" + sanitizeTableName(obs.getSatNo());
                long ts = obs.getFullTimestamp() != null ? obs.getFullTimestamp() : System.currentTimeMillis();
                tableGroup.computeIfAbsent(tableName, k -> new java.util.LinkedHashMap<>()).put(ts, obs);
            }

            for (java.util.Map.Entry<String, java.util.Map<Long, SatObservation>> entry : tableGroup.entrySet()) {
                String tableName = entry.getKey();
                java.util.Map<Long, SatObservation> obsMap = entry.getValue();
                if (obsMap.isEmpty()) continue;

                SatObservation firstObs = obsMap.values().iterator().next();

                // 拼装 USING 子句
                sqlBuilder.append(String.format(Locale.US,
                        "%s USING %s.%s TAGS ('%s', '%s') VALUES ",
                        tableName, database, STABLE_NAME,
                        sanitizeStr(firstObs.getStationId()), sanitizeStr(firstObs.getSatNo())
                ));

                // 拼装该表的所有安全 VALUES
                for (java.util.Map.Entry<Long, SatObservation> row : obsMap.entrySet()) {
                    long ts = row.getKey();
                    SatObservation obs = row.getValue();

                    sqlBuilder.append(String.format(Locale.US,
                            "(%d, '%s', %d, '%s', '%s', '%s', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) ",
                            ts,
                            sanitizeStr(obs.getFullObservationTimeStr()),
                            obs.getTimestamp() != null ? obs.getTimestamp() : 0L,
                            sanitizeStr(obs.getC1()), sanitizeStr(obs.getC2()), "",
                            formatDouble(obs.getPseudorangeP1()), formatDouble(obs.getPseudorangeP2()), "0.0",
                            formatDouble(obs.getPhaseL1()), "0.0", "0.0",
                            formatDouble(obs.getSnr()), "0.0", "0.0",
                            formatDouble(obs.getElevation()), formatDouble(obs.getAzimuth())
                    ));
                }
            }

            tdengineUtil.executeUpdate(sqlBuilder.toString());
        } catch (Exception e) {
            // 打印出引发错误的根本原因 (Cause)，有助于后续排错
            log.error("❌ 批量存储卫星观测数据单片失败: {} | Cause: {}", e.getMessage(), e.getCause() != null ? e.getCause().getMessage() : "None");
        }
    }

    private String sanitizeTableName(String name) {
        return name == null ? "unknown" : name.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    private String sanitizeStr(String val) {
        return val == null ? "" : val.replace("'", "\\'");
    }

    /**
     * 安全处理 Double 异常值，防止 NaN 破坏 SQL 语法
     */
    private String formatDouble(Double val) {
        if (val == null || val.isNaN() || val.isInfinite()) {
            return "NULL";
        }
        return String.format(Locale.US, "%f", val);
    }

    @Override
    public List<SatObservation> queryByTimeRange(String stationId, long startTime, long endTime) {
        return Collections.emptyList();
    }

    @Override
    public List<SatObservation> queryByDate(String stationId, LocalDate date) {
        return Collections.emptyList();
    }

    @Override
    public SatObservation queryByUniqueKey(String stationId, String obsUniqueKey) {
        return null;
    }

    @Override
    public int deleteByTimeRange(String stationId, long startTime, long endTime) {
        return 0;
    }

    @Override
    public String getStatistics() {
        return String.format("{\"initialized\": %b, \"target_db\": \"%s\"}", this.initialized, this.database);
    }
}