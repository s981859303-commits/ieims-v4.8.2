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
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * TDengine 卫星观测数据存储服务实现（修复版）
 *
 * 超级表结构：st_sat_observation
 * - ts: 主时间戳（接收时间）
 * - epoch_time: 历元时间（GNSS时间，仅时分秒毫秒）
 * - sat_no: 卫星编号
 * - sat_system: 卫星系统
 * - elevation: 仰角
 * - azimuth: 方位角
 * - snr: 信噪比
 * - pseudorange_p1: 伪距P1
 * - phase_l1: 载波相位L1
 * - pseudorange_p2: 伪距P2
 * - phase_p2: 载波相位P2
 * - c1: 信号代码1
 * - c2: 信号代码2
 * - data_source: 数据来源（GSV/RTCM/FUSED）
 * - observation_date: 观测日期
 * - obs_time: 观测时间（时分秒毫秒）
 * - obs_unique_key: 唯一键
 * - date_source: 日期来源
 *
 * 子表命名规则：st_sat_observation_{stationId}
 *
 * @version 2.1 - 2026-04-02 修复严重bug
 */
@Service
public class TDengineSatObservationServiceImpl implements ISatObservationStorageService {

    private static final Logger logger = LoggerFactory.getLogger(TDengineSatObservationServiceImpl.class);

    // ==================== 配置参数 ====================

    @Value("${tdengine.database:gnss}")
    private String database;

    @Value("${tdengine.supertable.sat_observation:st_sat_observation}")
    private String superTable;

    @Value("${tdengine.batchSize:100}")
    private int batchSize;

    // ==================== 依赖注入 ====================

    @Autowired
    private JdbcTemplate jdbcTemplate;

    // ==================== 统计计数器 ====================

    private final AtomicLong totalInsertCount = new AtomicLong(0);
    private final AtomicLong totalQueryCount = new AtomicLong(0);
    private final AtomicLong totalErrorCount = new AtomicLong(0);

    // ==================== 子表缓存 ====================

    /** 已创建的子表缓存 */
    private final Set<String> createdTables = ConcurrentHashMap.newKeySet();

    // ==================== 时间格式化器 ====================

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss.SSS");
    private static final DateTimeFormatter DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    // ==================== 初始化 ====================

    /**
     * 初始化：确保超级表存在
     */
    @javax.annotation.PostConstruct
    public void init() {
        try {
            createSuperTableIfNotExists();
            logger.info("TDengine卫星观测数据存储服务初始化完成 - database: {}, superTable: {}", database, superTable);
        } catch (Exception e) {
            logger.error("初始化失败", e);
        }
    }

    // ==================== 单条保存 ====================

    @Override
    public void saveSatObservation(String stationId, SatObservation observation) {
        if (observation == null) {
            return;
        }

        if (stationId == null || stationId.isEmpty()) {
            stationId = observation.getStationId() != null ? observation.getStationId() : "default";
        }

        try {
            // 【修复】预处理观测数据 - 不再调用私有方法
            preprocessObservation(observation);

            // 确保子表存在
            String tableName = getTableName(stationId);
            ensureTableExists(stationId, tableName);

            // 插入数据
            String sql = buildInsertSql(tableName);
            Object[] params = buildInsertParams(observation);

            jdbcTemplate.update(sql, params);

            totalInsertCount.incrementAndGet();
            logger.debug("保存卫星观测数据成功: stationId={}, satNo={}", stationId, observation.getSatNo());

        } catch (Exception e) {
            totalErrorCount.incrementAndGet();
            logger.error("保存卫星观测数据失败: stationId={}, satNo={}, error={}",
                    stationId, observation.getSatNo(), e.getMessage(), e);
        }
    }

    // ==================== 批量保存 ====================

    @Override
    public void saveSatObservationBatch(String stationId, List<SatObservation> observations) {
        if (observations == null || observations.isEmpty()) {
            return;
        }

        if (stationId == null || stationId.isEmpty()) {
            stationId = "default";
        }

        try {
            // 【修复】预处理所有观测数据 - 不再调用私有方法
            for (SatObservation obs : observations) {
                preprocessObservation(obs);
            }

            // 确保子表存在
            String tableName = getTableName(stationId);
            ensureTableExists(stationId, tableName);

            // 【修复】使用正确的批量插入语法
            // TDengine 批量插入语法: INSERT INTO table VALUES (...), (...), ...
            int successCount = insertBatch(tableName, observations);

            totalInsertCount.addAndGet(successCount);
            logger.info("批量保存卫星观测数据: stationId={}, total={}, success={}",
                    stationId, observations.size(), successCount);

        } catch (Exception e) {
            totalErrorCount.incrementAndGet();
            logger.error("批量保存卫星观测数据失败: stationId={}, error={}", stationId, e.getMessage(), e);

            // 降级为逐条插入
            for (SatObservation obs : observations) {
                try {
                    saveSatObservation(stationId, obs);
                } catch (Exception ex) {
                    logger.warn("逐条保存失败: satNo={}", obs.getSatNo());
                }
            }
        }
    }

    /**
     * 【修复】批量插入 - 使用正确的 TDengine 批量插入语法
     */
    private int insertBatch(String tableName, List<SatObservation> observations) {
        if (observations == null || observations.isEmpty()) {
            return 0;
        }

        int successCount = 0;
        int total = observations.size();

        // 分批处理
        for (int i = 0; i < total; i += batchSize) {
            int end = Math.min(i + batchSize, total);
            List<SatObservation> batch = observations.subList(i, end);

            try {
                // 【修复】构建正确的批量插入 SQL
                // TDengine 语法: INSERT INTO table VALUES (v1, v2, ...), (v1, v2, ...)
                StringBuilder sql = new StringBuilder("INSERT INTO ");
                sql.append(tableName).append(" VALUES ");

                List<Object> allParams = new ArrayList<>();
                boolean first = true;

                for (SatObservation obs : batch) {
                    if (!first) {
                        sql.append(", ");
                    }
                    first = false;

                    sql.append("(");
                    sql.append("?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?");
                    sql.append(")");

                    Collections.addAll(allParams, buildInsertParams(obs));
                }

                if (!allParams.isEmpty()) {
                    jdbcTemplate.update(sql.toString(), allParams.toArray());
                    successCount += batch.size();
                }

            } catch (Exception e) {
                logger.error("批量插入失败: batch {}-{}, error={}", i, end, e.getMessage());

                // 降级为逐条插入
                for (SatObservation obs : batch) {
                    try {
                        String sql = buildInsertSql(tableName);
                        jdbcTemplate.update(sql, buildInsertParams(obs));
                        successCount++;
                    } catch (Exception ex) {
                        logger.warn("逐条插入失败: satNo={}", obs.getSatNo());
                    }
                }
            }
        }

        return successCount;
    }

    // ==================== 查询方法 ====================

    @Override
    public List<SatObservation> queryByTimeRange(String stationId, long startTime, long endTime) {
        if (stationId == null || startTime <= 0 || endTime <= 0) {
            return Collections.emptyList();
        }

        try {
            String tableName = getTableName(stationId);
            String sql = String.format(
                    "SELECT * FROM %s WHERE ts >= ? AND ts <= ? ORDER BY ts",
                    tableName
            );

            List<SatObservation> result = jdbcTemplate.query(
                    sql,
                    ps -> {
                        ps.setTimestamp(1, new Timestamp(startTime));
                        ps.setTimestamp(2, new Timestamp(endTime));
                    },
                    this::mapRowToObservation
            );

            totalQueryCount.addAndGet(result.size());
            return result;

        } catch (Exception e) {
            logger.error("查询观测数据失败: stationId={}, error={}", stationId, e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    @Override
    public List<SatObservation> queryByDate(String stationId, LocalDate date) {
        if (stationId == null || date == null) {
            return Collections.emptyList();
        }

        try {
            String tableName = getTableName(stationId);
            String sql = String.format(
                    "SELECT * FROM %s WHERE observation_date = ? ORDER BY ts",
                    tableName
            );

            List<SatObservation> result = jdbcTemplate.query(
                    sql,
                    ps -> ps.setString(1, date.format(DATE_FORMATTER)),
                    this::mapRowToObservation
            );

            totalQueryCount.addAndGet(result.size());
            return result;

        } catch (Exception e) {
            logger.error("按日期查询观测数据失败: stationId={}, date={}, error={}",
                    stationId, date, e.getMessage(), e);
            return Collections.emptyList();
        }
    }

    @Override
    public SatObservation queryByUniqueKey(String stationId, String obsUniqueKey) {
        if (stationId == null || obsUniqueKey == null || obsUniqueKey.isEmpty()) {
            return null;
        }

        try {
            String tableName = getTableName(stationId);
            String sql = String.format(
                    "SELECT * FROM %s WHERE obs_unique_key = ? LIMIT 1",
                    tableName
            );

            List<SatObservation> result = jdbcTemplate.query(
                    sql,
                    ps -> ps.setString(1, obsUniqueKey),
                    this::mapRowToObservation
            );

            if (!result.isEmpty()) {
                totalQueryCount.incrementAndGet();
                return result.get(0);
            }

            return null;

        } catch (Exception e) {
            logger.error("按唯一键查询观测数据失败: stationId={}, key={}, error={}",
                    stationId, obsUniqueKey, e.getMessage(), e);
            return null;
        }
    }

    // ==================== 删除方法 ====================

    @Override
    public int deleteByTimeRange(String stationId, long startTime, long endTime) {
        if (stationId == null || startTime <= 0 || endTime <= 0) {
            return 0;
        }

        try {
            String tableName = getTableName(stationId);
            String sql = String.format("DELETE FROM %s WHERE ts >= ? AND ts <= ?", tableName);

            return jdbcTemplate.update(
                    sql,
                    new Timestamp(startTime),
                    new Timestamp(endTime)
            );

        } catch (Exception e) {
            logger.error("删除观测数据失败: stationId={}, error={}", stationId, e.getMessage(), e);
            return 0;
        }
    }

    // ==================== 统计方法 ====================

    @Override
    public String getStatistics() {
        return String.format(
                "总插入: %d, 总查询: %d, 总错误: %d, 已创建表: %d",
                totalInsertCount.get(), totalQueryCount.get(), totalErrorCount.get(), createdTables.size()
        );
    }

    // ==================== 私有方法 ====================

    /**
     * 【修复】预处理观测数据 - 不再调用私有方法
     *
     * 原代码调用了私有方法：
     * - obs.calculateObsUniqueKey()
     * - obs.calculateFullTimestamp()
     *
     * 修复方案：直接设置必要的字段
     */
    private void preprocessObservation(SatObservation obs) {
        if (obs == null) {
            return;
        }

        // 【修复】设置唯一键 - 不调用私有方法
        if (obs.getObsUniqueKey() == null || obs.getObsUniqueKey().isEmpty()) {
            String uniqueKey = generateObsUniqueKey(obs);
            obs.setObsUniqueKey(uniqueKey);
        }

        // 【修复】设置完整时间戳 - 不调用私有方法
        if (obs.getTimestamp() == null) {
            obs.setTimestamp(System.currentTimeMillis());
        }

        // 设置观测日期
        if (obs.getObservationDate() == null) {
            obs.setObservationDate(LocalDate.now(ZoneOffset.UTC), SatObservation.DATE_SOURCE_SYSTEM);
        }
    }

    /**
     * 生成观测唯一键
     */
    private String generateObsUniqueKey(SatObservation obs) {
        StringBuilder sb = new StringBuilder();

        // 日期
        if (obs.getObservationDate() != null) {
            sb.append(obs.getObservationDate().format(DATE_FORMATTER));
        } else {
            sb.append("unknown");
        }
        sb.append("_");

        // 时间
        if (obs.getObservationTime() != null) {
            sb.append(obs.getObservationTime().format(DateTimeFormatter.ofPattern("HHmmssSSS")));
        } else if (obs.getEpochTime() != null) {
            sb.append(obs.getEpochTime());
        } else {
            sb.append("unknown");
        }
        sb.append("_");

        // 卫星编号
        if (obs.getSatNo() != null) {
            sb.append(obs.getSatNo());
        } else {
            sb.append("unknown");
        }

        return sb.toString();
    }

    /**
     * 获取子表名
     */
    private String getTableName(String stationId) {
        // 替换特殊字符
        String safeId = stationId.replaceAll("[^a-zA-Z0-9_]", "_");
        return superTable + "_" + safeId;
    }

    /**
     * 【修复】确保子表存在 - 原代码缺少此逻辑
     */
    private void ensureTableExists(String stationId, String tableName) {
        if (createdTables.contains(tableName)) {
            return;
        }

        try {
            // 检查表是否存在
            String checkSql = String.format("SHOW TABLES LIKE '%s'", tableName);
            List<Map<String, Object>> tables = jdbcTemplate.queryForList(checkSql);

            if (tables.isEmpty()) {
                // 创建子表
                createSubTable(stationId, tableName);
            }

            createdTables.add(tableName);

        } catch (Exception e) {
            logger.warn("检查/创建子表失败: tableName={}, error={}", tableName, e.getMessage());
            // 尝试直接创建
            try {
                createSubTable(stationId, tableName);
                createdTables.add(tableName);
            } catch (Exception ex) {
                logger.error("创建子表失败: tableName={}", tableName, ex);
            }
        }
    }

    /**
     * 创建超级表（如果不存在）
     */
    private void createSuperTableIfNotExists() {
        String sql = String.format(
                "CREATE STABLE IF NOT EXISTS %s.%s (" +
                        "ts TIMESTAMP, " +
                        "epoch_time BIGINT, " +
                        "sat_no NCHAR(16), " +
                        "sat_system NCHAR(16), " +
                        "elevation DOUBLE, " +
                        "azimuth DOUBLE, " +
                        "snr DOUBLE, " +
                        "pseudorange_p1 DOUBLE, " +
                        "phase_l1 DOUBLE, " +
                        "pseudorange_p2 DOUBLE, " +
                        "phase_p2 DOUBLE, " +
                        "c1 NCHAR(16), " +
                        "c2 NCHAR(16), " +
                        "data_source NCHAR(16), " +
                        "observation_date NCHAR(16), " +
                        "obs_time NCHAR(16), " +
                        "obs_unique_key NCHAR(64), " +
                        "date_source NCHAR(16)" +
                        ") TAGS (station_id NCHAR(64))",
                database, superTable
        );

        try {
            jdbcTemplate.execute(sql);
            logger.info("超级表已创建/确认: {}", superTable);
        } catch (Exception e) {
            logger.error("创建超级表失败: {}", superTable, e);
        }
    }

    /**
     * 创建子表
     */
    private void createSubTable(String stationId, String tableName) {
        String sql = String.format(
                "CREATE TABLE IF NOT EXISTS %s USING %s.%s TAGS ('%s')",
                tableName, database, superTable, stationId
        );

        jdbcTemplate.execute(sql);
        logger.info("子表已创建: {}", tableName);
    }

    /**
     * 构建插入 SQL
     */
    private String buildInsertSql(String tableName) {
        return String.format(
                "INSERT INTO %s VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                tableName
        );
    }

    /**
     * 构建插入参数
     */
    private Object[] buildInsertParams(SatObservation obs) {
        Object[] params = new Object[17];

        // ts - 主时间戳
        params[0] = obs.getTimestamp() != null ? new Timestamp(obs.getTimestamp()) : new Timestamp(System.currentTimeMillis());

        // epoch_time - 历元时间
        params[1] = obs.getEpochTime() != null ? obs.getEpochTime() : 0L;

        // sat_no - 卫星编号
        params[2] = obs.getSatNo() != null ? obs.getSatNo() : "";

        // sat_system - 卫星系统
        params[3] = obs.getSatSystem() != null ? obs.getSatSystem() : "";

        // elevation - 仰角
        params[4] = obs.getElevation() != null ? obs.getElevation() : 0.0;

        // azimuth - 方位角
        params[5] = obs.getAzimuth() != null ? obs.getAzimuth() : 0.0;

        // snr - 信噪比
        params[6] = obs.getSnr() != null ? obs.getSnr() : 0.0;

        // pseudorange_p1 - 伪距P1
        params[7] = obs.getPseudorangeP1() != null ? obs.getPseudorangeP1() : 0.0;

        // phase_l1 - 载波相位L1
        params[8] = obs.getPhaseL1() != null ? obs.getPhaseL1() : 0.0;

        // pseudorange_p2 - 伪距P2
        params[9] = obs.getPseudorangeP2() != null ? obs.getPseudorangeP2() : 0.0;

        // phase_p2 - 载波相位P2
        params[10] = obs.getPhaseP2() != null ? obs.getPhaseP2() : 0.0;

        // c1 - 信号代码1
        params[11] = obs.getC1() != null ? obs.getC1() : "";

        // c2 - 信号代码2
        params[12] = obs.getC2() != null ? obs.getC2() : "";

        // data_source - 数据来源
        params[13] = obs.getDataSource() != null ? obs.getDataSource() : "";

        // observation_date - 观测日期
        params[14] = obs.getObservationDate() != null ? obs.getObservationDate().format(DATE_FORMATTER) : "";

        // obs_time - 观测时间
        params[15] = obs.getObservationTime() != null ? obs.getObservationTime().format(TIME_FORMATTER) : "";

        // obs_unique_key - 唯一键
        params[16] = obs.getObsUniqueKey() != null ? obs.getObsUniqueKey() : "";

        return params;
    }

    /**
     * 【修复】映射 ResultSet 到 SatObservation
     *
     * 原代码问题：
     * - BUG-5: setObsTime() 方法不存在，应为 setObservationTime()
     * - BUG-6: epoch_time NULL 值处理不当
     */
    private SatObservation mapRowToObservation(ResultSet rs, int rowNum) throws SQLException {
        SatObservation obs = new SatObservation();

        // ts
        Timestamp ts = rs.getTimestamp("ts");
        if (ts != null) {
            obs.setTimestamp(ts.getTime());
        }

        // 【修复】epoch_time - 正确处理 NULL 值
        long epochTime = rs.getLong("epoch_time");
        if (!rs.wasNull()) {
            obs.setEpochTime(epochTime);
        } else {
            obs.setEpochTime(null);
        }

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

        // observation_date
        String dateStr = rs.getString("observation_date");
        if (dateStr != null && !dateStr.isEmpty()) {
            try {
                obs.setObservationDate(LocalDate.parse(dateStr, DATE_FORMATTER));
            } catch (Exception e) {
                logger.warn("解析日期失败: {}", dateStr);
            }
        }

        // 【修复】obs_time - 使用正确的方法名 setObservationTime
        String timeStr = rs.getString("obs_time");
        if (timeStr != null && !timeStr.isEmpty()) {
            try {
                obs.setObservationTime(LocalTime.parse(timeStr, TIME_FORMATTER));
            } catch (Exception e) {
                logger.warn("解析时间失败: {}", timeStr);
            }
        }

        obs.setObsUniqueKey(rs.getString("obs_unique_key"));
        obs.setDateSource(rs.getString("date_source"));

        return obs;
    }
}
