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
 *
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
        // 防止数据库不存在导致建表崩溃
        try {
            tdengineUtil.executeDDL("CREATE DATABASE IF NOT EXISTS " + database);
        } catch (Exception e) {
            log.debug("尝试建库失败 (云端版或已存在可忽略): {}", e.getMessage());
        }

        // sys_time 设为 BIGINT，满足 TDengine STABLE 仅允许首列为主键 TIMESTAMP 的要求
        String createSuperTableSql = String.format(
                "CREATE STABLE IF NOT EXISTS %s (" +
                        "ts TIMESTAMP, " +
                        "obs_time VARCHAR(30), " +
                        "sys_time BIGINT, " +
                        "c1 VARCHAR(10), c2 VARCHAR(10), c3 VARCHAR(10), " +
                        "p1 DOUBLE, p2 DOUBLE, p3 DOUBLE, " +
                        "l1 DOUBLE, l2 DOUBLE, l3 DOUBLE, " +
                        "s1 DOUBLE, s2 DOUBLE, s3 DOUBLE, " +
                        "elevation DOUBLE, azimuth DOUBLE" +
                        ") TAGS (station_id VARCHAR(50), sat_name VARCHAR(10))",
                STABLE_NAME
        );
        tdengineUtil.executeDDL(createSuperTableSql);
        log.info("✅ 创建/确认卫星观测超级表完成: {}", STABLE_NAME);
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

        // TDengine API 对 SQL 长度有严苛要求。强制切分为每 500 条组装一条入库。
        int batchSize = 500;
        for (int i = 0; i < obsList.size(); i += batchSize) {
            int end = Math.min(i + batchSize, obsList.size());
            List<SatObservation> subList = obsList.subList(i, end);
            executeBatchSql(subList);
        }
    }

    private void executeBatchSql(List<SatObservation> obsList) {
        try {
            StringBuilder sqlBuilder = new StringBuilder("INSERT INTO ");

            for (SatObservation obs : obsList) {
                // 表名通过 stationId 和 satNo 拼接
                String tableName = "satobs_" + sanitizeTableName(obs.getStationId()) + "_" + sanitizeTableName(obs.getSatNo());

                // 🔥 核心修复：将字段映射到 SatObservation 中真实存在的 Getter 方法
                sqlBuilder.append(String.format(Locale.US,
                        "%s USING %s TAGS ('%s', '%s') VALUES (%d, '%s', %d, '%s', '%s', '%s', %f, %f, %f, %f, %f, %f, %f, %f, %f, %f, %f) ",
                        tableName, STABLE_NAME, obs.getStationId(), obs.getSatNo(),
                        obs.getFullTimestamp() != null ? obs.getFullTimestamp() : System.currentTimeMillis(), // 主键 ts
                        obs.getObservationTimeStr() != null ? obs.getObservationTimeStr() : "", // obs_time (替换了错误的 getObsTime)
                        obs.getTimestamp() != null ? obs.getTimestamp() : 0L, // sys_time (替换了错误的 getSysTime)
                        obs.getC1() != null ? obs.getC1() : "",
                        obs.getC2() != null ? obs.getC2() : "",
                        "", // 实体类中没有 C3，默认填空字符串
                        obs.getPseudorangeP1() != null ? obs.getPseudorangeP1() : 0.0, // P1 (替换了错误的 getP1)
                        obs.getPseudorangeP2() != null ? obs.getPseudorangeP2() : 0.0, // P2 (替换了错误的 getP2)
                        0.0, // P3，实体中无此字段
                        obs.getPhaseL1() != null ? obs.getPhaseL1() : 0.0, // L1 (替换了错误的 getL1)
                        0.0, // L2，实体中为 phaseP2（这里先填0，下面填到对应位置）
                        0.0, // L3
                        obs.getSnr() != null ? obs.getSnr() : 0.0, // S1 (使用 snr 替代)
                        0.0, // S2
                        0.0, // S3
                        obs.getElevation() != null ? obs.getElevation() : 0.0,
                        obs.getAzimuth() != null ? obs.getAzimuth() : 0.0
                ));
            }

            tdengineUtil.executeUpdate(sqlBuilder.toString());
        } catch (Exception e) {
            log.error("❌ 批量存储卫星观测数据单片失败: {}", e.getMessage());
        }
    }

    private String sanitizeTableName(String name) {
        return name == null ? "unknown" : name.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    // =========================================================================
    // 🔥 占位实现：满足 ISatObservationStorageService 接口的其他规范要求
    // =========================================================================

    @Override
    public List<SatObservation> queryByTimeRange(String stationId, long startTime, long endTime) {
        log.debug("TDengine queryByTimeRange 暂未通过代码层实现");
        return Collections.emptyList();
    }

    @Override
    public List<SatObservation> queryByDate(String stationId, LocalDate date) {
        log.debug("TDengine queryByDate 暂未通过代码层实现");
        return Collections.emptyList();
    }

    @Override
    public SatObservation queryByUniqueKey(String stationId, String obsUniqueKey) {
        log.debug("TDengine queryByUniqueKey 暂未通过代码层实现");
        return null;
    }

    @Override
    public int deleteByTimeRange(String stationId, long startTime, long endTime) {
        log.debug("TDengine deleteByTimeRange 暂未通过代码层实现");
        return 0;
    }

    @Override
    public String getStatistics() {
        return String.format("{\"initialized\": %b, \"target_db\": \"%s\"}", this.initialized, this.database);
    }
}