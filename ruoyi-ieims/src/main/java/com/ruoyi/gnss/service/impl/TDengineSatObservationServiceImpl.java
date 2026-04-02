package com.ruoyi.gnss.service.impl;

import com.ruoyi.gnss.domain.SatObservation;
import com.ruoyi.gnss.service.ISatObservationStorageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.PreparedStatementCreator;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * TDengine 卫星观测数据存储服务实现（增强版 - 支持日期字段）
 *
 * 超级表结构：st_sat_observation
 * - 改造前：ts, epoch_time, sat_no, sat_system, elevation, azimuth, snr,
 *           pseudorange_p1, phase_l1, pseudorange_p2, phase_p2, c1, c2, data_source
 * - 改造后：新增 observation_date, obs_time, obs_unique_key, date_source
 *
 * 标签：station_id
 *
 * 【重构说明】
 * 1. 新增 observation_date 字段存储观测日期
 * 2. 新增 obs_time 字段存储观测时间（时分秒毫秒）
 * 3. 新增 obs_unique_key 字段作为唯一标识
 * 4. 新增 date_source 字段标记日期来源
 * 5. 支持批量插入优化
 * 6. 支持去重和幂等性
 *
 * @author GNSS Team
 * @version 2.0 - 2026-04-02 添加日期字段支持
 */
@Service
public class TDengineSatObservationServiceImpl implements ISatObservationStorageService {

    private static final Logger logger = LoggerFactory.getLogger(TDengineSatObservationServiceImpl.class);

    // ==================== 常量定义 ====================

    /** 超级表名称 */
    private static final String SUPER_TABLE = "st_sat_observation";

    /** 日期格式化器 */
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /** 时间格式化器 */
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");

    /** 日期时间格式化器 */
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    /** 批量插入大小 */
    private static final int BATCH_INSERT_SIZE = 500;

    /** 最大重试次数 */
    private static final int MAX_RETRY_COUNT = 3;

    /** 重试间隔（毫秒） */
    private static final long RETRY_INTERVAL_MS = 100;

    // ==================== 依赖注入 ====================

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${gnss.storage.database:gnss}")
    private String database;

    @Value("${gnss.storage.batchSize:500}")
    private int batchSize;

    @Value("${gnss.storage.enableDedup:true}")
    private boolean enableDedup;

    // ==================== 统计计数器 ====================

    private final AtomicLong insertCount = new AtomicLong(0);
    private final AtomicLong errorCount = new AtomicLong(0);
    private final AtomicLong dedupCount = new AtomicLong(0);

    // ==================== 公共接口实现 ====================

    /**
     * 保存单条卫星观测数据
     *
     * @param stationId   站点ID
     * @param observation 观测数据
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void saveSatObservation(String stationId, SatObservation observation) {
        if (observation == null) {
            return;
        }

        saveSatObservationBatch(stationId, Collections.singletonList(observation));
    }

    /**
     * 批量保存卫星观测数据
     *
     * @param stationId    站点ID
     * @param observations 观测数据列表
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void saveSatObservationBatch(String stationId, List<SatObservation> observations) {
        if (observations == null || observations.isEmpty()) {
            return;
        }

        // 数据校验和预处理
        List<SatObservation> validObservations = preprocessObservations(stationId, observations);
        if (validObservations.isEmpty()) {
            return;
        }

        // 去重处理
        if (enableDedup) {
            validObservations = deduplicateObservations(validObservations);
        }

        // 分批插入
        int totalSize = validObservations.size();
        int successCount = 0;

        for (int i = 0; i < totalSize; i += BATCH_INSERT_SIZE) {
            int end = Math.min(i + BATCH_INSERT_SIZE, totalSize);
            List<SatObservation> batch = validObservations.subList(i, end);

            try {
                insertBatchWithRetry(stationId, batch);
                successCount += batch.size();
            } catch (Exception e) {
                logger.error("站点 {} 批量插入失败 [{}/{}]: {}",
                        stationId, i, totalSize, e.getMessage());
                errorCount.incrementAndGet();

                // 尝试逐条插入
                for (SatObservation obs : batch) {
                    try {
                        insertSingle(stationId, obs);
                        successCount++;
                    } catch (Exception ex) {
                        logger.warn("站点 {} 单条插入失败: satNo={}, error={}",
                                stationId, obs.getSatNo(), ex.getMessage());
                    }
                }
            }
        }

        insertCount.addAndGet(successCount);
        logger.debug("站点 {} 批量保存完成: 成功 {}/{} 条", stationId, successCount, totalSize);
    }

    /**
     * 按时间范围查询观测数据
     *
     * @param stationId 站点ID
     * @param startTime 开始时间
     * @param endTime   结束时间
     * @return 观测数据列表
     */
    @Override
    public List<SatObservation> queryByTimeRange(String stationId, long startTime, long endTime) {
        String sql = String.format(
                "SELECT ts, epoch_time, sat_no, sat_system, elevation, azimuth, snr, " +
                        "       pseudorange_p1, phase_l1, pseudorange_p2, phase_p2, c1, c2, data_source, " +
                        "       observation_date, obs_time, obs_unique_key, date_source " +
                        "FROM %s WHERE station_id = ? AND ts >= ? AND ts < ? ORDER BY ts",
                SUPER_TABLE);

        try {
            return jdbcTemplate.query(sql,
                    ps -> {
                        ps.setString(1, stationId);
                        ps.setLong(2, startTime);
                        ps.setLong(3, endTime);
                    },
                    this::mapRowToObservation);

        } catch (Exception e) {
            logger.error("查询观测数据失败: stationId={}, error={}", stationId, e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * 按日期查询观测数据
     *
     * @param stationId 站点ID
     * @param date      观测日期
     * @return 观测数据列表
     */
    @Override
    public List<SatObservation> queryByDate(String stationId, LocalDate date) {
        String sql = String.format(
                "SELECT ts, epoch_time, sat_no, sat_system, elevation, azimuth, snr, " +
                        "       pseudorange_p1, phase_l1, pseudorange_p2, phase_p2, c1, c2, data_source, " +
                        "       observation_date, obs_time, obs_unique_key, date_source " +
                        "FROM %s WHERE station_id = ? AND observation_date = ? ORDER BY ts",
                SUPER_TABLE);

        try {
            return jdbcTemplate.query(sql,
                    ps -> {
                        ps.setString(1, stationId);
                        ps.setString(2, date.format(DATE_FORMATTER));
                    },
                    this::mapRowToObservation);

        } catch (Exception e) {
            logger.error("按日期查询观测数据失败: stationId={}, date={}, error={}",
                    stationId, date, e.getMessage());
            return Collections.emptyList();
        }
    }

    /**
     * 按唯一键查询观测数据
     *
     * @param stationId    站点ID
     * @param obsUniqueKey 唯一键
     * @return 观测数据
     */
    @Override
    public SatObservation queryByUniqueKey(String stationId, String obsUniqueKey) {
        String sql = String.format(
                "SELECT ts, epoch_time, sat_no, sat_system, elevation, azimuth, snr, " +
                        "       pseudorange_p1, phase_l1, pseudorange_p2, phase_p2, c1, c2, data_source, " +
                        "       observation_date, obs_time, obs_unique_key, date_source " +
                        "FROM %s WHERE station_id = ? AND obs_unique_key = ?",
                SUPER_TABLE);

        try {
            List<SatObservation> results = jdbcTemplate.query(sql,
                    ps -> {
                        ps.setString(1, stationId);
                        ps.setString(2, obsUniqueKey);
                    },
                    this::mapRowToObservation);

            return results.isEmpty() ? null : results.get(0);

        } catch (Exception e) {
            logger.error("按唯一键查询观测数据失败: key={}, error={}", obsUniqueKey, e.getMessage());
            return null;
        }
    }

    /**
     * 删除指定时间范围的数据
     *
     * @param stationId 站点ID
     * @param startTime 开始时间
     * @param endTime   结束时间
     * @return 删除条数
     */
    @Override
    public int deleteByTimeRange(String stationId, long startTime, long endTime) {
        String sql = String.format(
                "DELETE FROM %s WHERE station_id = ? AND ts >= ? AND ts < ?",
                SUPER_TABLE);

        try {
            return jdbcTemplate.update(sql, stationId, startTime, endTime);
        } catch (Exception e) {
            logger.error("删除观测数据失败: stationId={}, error={}", stationId, e.getMessage());
            return 0;
        }
    }

    /**
     * 获取统计信息
     *
     * @return 统计信息字符串
     */
    @Override
    public String getStatistics() {
        return String.format("插入=%d, 错误=%d, 去重=%d",
                insertCount.get(), errorCount.get(), dedupCount.get());
    }

    // ==================== 私有方法 ====================

    /**
     * 预处理观测数据
     */
    private List<SatObservation> preprocessObservations(String stationId, List<SatObservation> observations) {
        List<SatObservation> valid = new ArrayList<>();

        for (SatObservation obs : observations) {
            // 校验必填字段
            if (obs.getSatNo() == null || obs.getSatNo().isEmpty()) {
                logger.warn("卫星编号为空，跳过: {}", obs);
                continue;
            }

            // 设置站点ID
            if (obs.getStationId() == null) {
                obs.setStationId(stationId);
            }

            // 设置时间戳
            if (obs.getTimestamp() == null) {
                obs.setTimestamp(System.currentTimeMillis());
            }

            // 计算唯一键
            if (obs.getObsUniqueKey() == null || obs.getObsUniqueKey().isEmpty()) {
                obs.calculateObsUniqueKey();
            }

            // 计算完整时间戳
            if (obs.getFullTimestamp() == null) {
                obs.calculateFullTimestamp();
            }

            // 设置默认日期
            if (obs.getObservationDate() == null) {
                obs.setObservationDate(LocalDate.now());
                obs.setDateSource("SYSTEM");
                logger.debug("观测数据缺少日期，使用系统日期: satNo={}", obs.getSatNo());
            }

            valid.add(obs);
        }

        return valid;
    }

    /**
     * 去重处理
     */
    private List<SatObservation> deduplicateObservations(List<SatObservation> observations) {
        Map<String, SatObservation> uniqueMap = new LinkedHashMap<>();

        for (SatObservation obs : observations) {
            String key = obs.getObsUniqueKey();
            if (key == null) {
                continue;
            }

            SatObservation existing = uniqueMap.get(key);
            if (existing == null) {
                uniqueMap.put(key, obs);
            } else {
                // 合并数据：保留非空值
                mergeObservation(existing, obs);
                dedupCount.incrementAndGet();
            }
        }

        return new ArrayList<>(uniqueMap.values());
    }

    /**
     * 合并观测数据（保留非空值）
     */
    private void mergeObservation(SatObservation target, SatObservation source) {
        if (target.getElevation() == null && source.getElevation() != null) {
            target.setElevation(source.getElevation());
        }
        if (target.getAzimuth() == null && source.getAzimuth() != null) {
            target.setAzimuth(source.getAzimuth());
        }
        if (target.getSnr() == null && source.getSnr() != null) {
            target.setSnr(source.getSnr());
        }
        if (target.getPseudorangeP1() == null && source.getPseudorangeP1() != null) {
            target.setPseudorangeP1(source.getPseudorangeP1());
        }
        if (target.getPhaseL1() == null && source.getPhaseL1() != null) {
            target.setPhaseL1(source.getPhaseL1());
        }
        if (target.getPseudorangeP2() == null && source.getPseudorangeP2() != null) {
            target.setPseudorangeP2(source.getPseudorangeP2());
        }
        if (target.getPhaseP2() == null && source.getPhaseP2() != null) {
            target.setPhaseP2(source.getPhaseP2());
        }
    }

    /**
     * 批量插入（带重试）
     */
    private void insertBatchWithRetry(String stationId, List<SatObservation> observations) {
        Exception lastException = null;

        for (int retry = 0; retry < MAX_RETRY_COUNT; retry++) {
            try {
                insertBatch(stationId, observations);
                return;
            } catch (Exception e) {
                lastException = e;
                if (retry < MAX_RETRY_COUNT - 1) {
                    try {
                        Thread.sleep(RETRY_INTERVAL_MS * (retry + 1));
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        break;
                    }
                }
            }
        }

        throw new RuntimeException("批量插入失败: " + lastException.getMessage(), lastException);
    }

    /**
     * 批量插入
     */
    private void insertBatch(String stationId, List<SatObservation> observations) {
        // 构建批量插入SQL
        // TDengine 使用 INSERT INTO ... VALUES 语法
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ");

        for (int i = 0; i < observations.size(); i++) {
            SatObservation obs = observations.get(i);
            String tableName = getSubTableName(stationId, obs.getSatNo());

            if (i > 0) {
                sql.append(" ");
            }

            sql.append(tableName).append(" VALUES (");

            // ts - 时间戳
            sql.append(obs.getTimestamp());

            // epoch_time - 历元时间
            sql.append(", ").append(obs.getEpochTime() != null ? obs.getEpochTime() : "NULL");

            // sat_no - 卫星编号
            sql.append(", '").append(escapeString(obs.getSatNo())).append("'");

            // sat_system - 卫星系统
            sql.append(", '").append(escapeString(obs.getSatSystem())).append("'");

            // elevation - 仰角
            sql.append(", ").append(formatDouble(obs.getElevation()));

            // azimuth - 方位角
            sql.append(", ").append(formatDouble(obs.getAzimuth()));

            // snr - 信噪比
            sql.append(", ").append(formatDouble(obs.getSnr()));

            // pseudorange_p1 - 伪距P1
            sql.append(", ").append(formatDouble(obs.getPseudorangeP1()));

            // phase_l1 - 相位L1
            sql.append(", ").append(formatDouble(obs.getPhaseL1()));

            // pseudorange_p2 - 伪距P2
            sql.append(", ").append(formatDouble(obs.getPseudorangeP2()));

            // phase_p2 - 相位P2
            sql.append(", ").append(formatDouble(obs.getPhaseP2()));

            // c1 - 信号代码1
            sql.append(", '").append(escapeString(obs.getC1())).append("'");

            // c2 - 信号代码2
            sql.append(", '").append(escapeString(obs.getC2())).append("'");

            // data_source - 数据来源
            sql.append(", '").append(escapeString(obs.getDataSource())).append("'");

            // 【新增】observation_date - 观测日期
            sql.append(", '").append(obs.getObservationDate().format(DATE_FORMATTER)).append("'");

            // 【新增】obs_time - 观测时间
            sql.append(", '").append(formatObsTime(obs.getObsTime())).append("'");

            // 【新增】obs_unique_key - 唯一键
            sql.append(", '").append(escapeString(obs.getObsUniqueKey())).append("'");

            // 【新增】date_source - 日期来源
            sql.append(", '").append(escapeString(obs.getDateSource())).append("'");

            sql.append(")");
        }

        jdbcTemplate.execute(sql.toString());
    }

    /**
     * 单条插入
     */
    private void insertSingle(String stationId, SatObservation obs) {
        String tableName = getSubTableName(stationId, obs.getSatNo());

        String sql = String.format(
                "INSERT INTO %s VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                tableName);

        jdbcTemplate.update(sql,
                obs.getTimestamp(),
                obs.getEpochTime(),
                obs.getSatNo(),
                obs.getSatSystem(),
                obs.getElevation(),
                obs.getAzimuth(),
                obs.getSnr(),
                obs.getPseudorangeP1(),
                obs.getPhaseL1(),
                obs.getPseudorangeP2(),
                obs.getPhaseP2(),
                obs.getC1(),
                obs.getC2(),
                obs.getDataSource(),
                obs.getObservationDate().format(DATE_FORMATTER),
                formatObsTime(obs.getObsTime()),
                obs.getObsUniqueKey(),
                obs.getDateSource()
        );
    }

    /**
     * 获取子表名称
     */
    private String getSubTableName(String stationId, String satNo) {
        // 使用站点ID和卫星编号组合作为子表名
        // 格式：t_obs_{stationId}_{satNo}
        String safeStationId = sanitizeTableName(stationId);
        String safeSatNo = sanitizeTableName(satNo);
        return String.format("t_obs_%s_%s", safeStationId, safeSatNo);
    }

    /**
     * 清理表名（移除特殊字符）
     */
    private String sanitizeTableName(String name) {
        if (name == null) {
            return "unknown";
        }
        return name.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    /**
     * 转义字符串
     */
    private String escapeString(String s) {
        if (s == null) {
            return "";
        }
        return s.replace("'", "''");
    }

    /**
     * 格式化 Double 值
     */
    private String formatDouble(Double d) {
        if (d == null) {
            return "NULL";
        }
        return String.format("%.6f", d);
    }

    /**
     * 格式化观测时间
     */
    private String formatObsTime(java.time.LocalTime time) {
        if (time == null) {
            return "00:00:00.000";
        }
        return time.format(TIME_FORMATTER);
    }

    /**
     * 将 ResultSet 映射为 SatObservation 对象
     */
    private SatObservation mapRowToObservation(java.sql.ResultSet rs, int rowNum) throws SQLException {
        SatObservation obs = new SatObservation();

        obs.setTimestamp(rs.getLong("ts"));
        obs.setEpochTime(rs.getLong("epoch_time"));
        obs.setSatNo(rs.getString("sat_no"));
        obs.setSatSystem(rs.getString("sat_system"));
        obs.setElevation(rs.getDouble("elevation"));
        obs.setAzimuth(rs.getDouble("azimuth"));
        obs.setSnr(rs.getDouble("snr"));
        obs.setPseudorangeP1(rs.getDouble("pseudorange_p1"));
        obs.setPhaseL1(rs.getDouble("phase_l1"));
        obs.setPseudorangeP2(rs.getDouble("pseudorange_p2"));
        obs.setPhaseP2(rs.getDouble("phase_p2"));
        obs.setC1(rs.getString("c1"));
        obs.setC2(rs.getString("c2"));
        obs.setDataSource(rs.getString("data_source"));

        // 【新增】日期字段
        String dateStr = rs.getString("observation_date");
        if (dateStr != null && !dateStr.isEmpty()) {
            obs.setObservationDate(LocalDate.parse(dateStr, DATE_FORMATTER));
        }

        String timeStr = rs.getString("obs_time");
        if (timeStr != null && !timeStr.isEmpty()) {
            obs.setObsTime(java.time.LocalTime.parse(timeStr, TIME_FORMATTER));
        }

        obs.setObsUniqueKey(rs.getString("obs_unique_key"));
        obs.setDateSource(rs.getString("date_source"));

        return obs;
    }
}
