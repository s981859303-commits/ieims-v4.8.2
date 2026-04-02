package com.ruoyi.gnss.service.impl;

import com.ruoyi.gnss.domain.NmeaRecord;
import com.ruoyi.gnss.service.INmeaStorageService;
import com.ruoyi.user.comm.core.tdengine.TDengineUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * TDengine NMEA 存储服务实现
 *
 * <p>
 * 优化内容：
 * 1. 缓存表名，避免重复计算
 * 2. 使用 volatile 修饰 initialized 保证可见性
 * 3. 添加统计计数器
 * 4. 支持多站点
 * 5. 【核心修复】使用 JdbcTemplate 的批量 PreparedStatement 替代字符串拼接，彻底杜绝 SQL 注入并大幅提升批量写入性能
 * </p>
 *
 * @author GNSS Team
 */
@Service
@ConditionalOnProperty(name = "gnss.tdengine.enabled", havingValue = "true", matchIfMissing = false)
public class TDengineNmeaStorageServiceImpl implements INmeaStorageService {

    private static final Logger logger = LoggerFactory.getLogger(TDengineNmeaStorageServiceImpl.class);

    // ==================== 配置参数 ====================

    @Value("${gnss.tdengine.database:ieims}")
    private String database;

    @Value("${gnss.parser.stationId:8900_1}")
    private String defaultStationId;

    @Value("${gnss.tdengine.tablePrefix:nmea_}")
    private String tablePrefix;

    @Value("${gnss.nmea.batchSize:100}")
    private int batchSize;

    // ==================== 常量 ====================

    private static final String STABLE_NAME = "st_nmea_raw";

    // ==================== 依赖注入 ====================

    @Resource
    private TDengineUtil tdengineUtil;

    // 引入 Spring 原生的 JdbcTemplate 进行高性能安全的批量插入
    // (通常在配置 TDengine 数据源时，系统中会自动装配好对应的 JdbcTemplate)
    @Resource
    private JdbcTemplate jdbcTemplate;

    // ==================== 缓存变量（优化点） ====================

    /** 缓存的默认表名 */
    private String cachedDefaultTableName;

    /** 缓存的净化后默认站点ID */
    private String cachedDefaultSanitizedStationId;

    // ==================== 状态变量 ====================

    /** 使用 volatile 保证多线程可见性 */
    private volatile boolean initialized = false;

    // ==================== 统计计数器 ====================

    private final AtomicLong savedCount = new AtomicLong(0);
    private final AtomicLong failedCount = new AtomicLong(0);
    private final AtomicLong batchSavedCount = new AtomicLong(0);

    // ==================== 初始化 ====================

    @PostConstruct
    public void init() {
        try {
            // 缓存默认站点ID和表名
            this.cachedDefaultSanitizedStationId = sanitizeTableName(defaultStationId);
            this.cachedDefaultTableName = tablePrefix + cachedDefaultSanitizedStationId;

            initTables();
            initialized = true;
            logger.info("TDengine NMEA 存储服务初始化成功，默认站点: {}, 默认表: {}",
                    defaultStationId, cachedDefaultTableName);
        } catch (Exception e) {
            logger.error("TDengine NMEA 存储服务初始化失败: {}", e.getMessage());
        }
    }

    /**
     * 初始化超级表和默认子表
     */
    private void initTables() {
        tdengineUtil.executeDDL("USE " + database);

        // 创建超级表
        String createStableSql = String.format(
                "CREATE STABLE IF NOT EXISTS %s (" +
                        "ts TIMESTAMP, nmea_type VARCHAR(10), raw_content NCHAR(512)" +
                        ") TAGS (station_id VARCHAR(50))",
                STABLE_NAME
        );
        tdengineUtil.executeDDL(createStableSql);

        // 创建默认子表
        String createTableSql = String.format(
                "CREATE TABLE IF NOT EXISTS %s USING %s TAGS ('%s')",
                cachedDefaultTableName, STABLE_NAME, defaultStationId
        );
        tdengineUtil.executeDDL(createTableSql);

        logger.info("创建 NMEA 表: {}", cachedDefaultTableName);
    }

    // ==================== 接口实现 ====================

    @Override
    public boolean isInitialized() {
        return initialized;
    }

    @Override
    public boolean saveNmeaData(NmeaRecord record) {
        return saveNmeaData(defaultStationId, record);
    }

    @Override
    public boolean saveNmeaData(String stationId, NmeaRecord record) {
        if (!initialized || record == null || record.getRawContent() == null) {
            return false;
        }

        try {
            String tableName = getTableName(stationId);
            long timestamp = record.getReceivedTime() != null ?
                    record.getReceivedTime().getTime() : System.currentTimeMillis();

            String nmeaType = extractNmeaType(record.getRawContent());

            // 预编译参数化查询，防止单条 SQL 注入
            String insertSql = String.format(
                    "INSERT INTO %s (ts, nmea_type, raw_content) VALUES (?, ?, ?)",
                    tableName
            );

            tdengineUtil.executeUpdate(insertSql, timestamp, nmeaType, record.getRawContent());
            savedCount.incrementAndGet();

            if (logger.isTraceEnabled()) {
                logger.trace("NMEA 数据已存储: {}", nmeaType);
            }
            return true;

        } catch (Exception e) {
            failedCount.incrementAndGet();
            long failCount = failedCount.get();
            if (failCount % 100 == 1) {
                logger.error("存储 NMEA 数据失败 [累计失败: {}]: {}", failCount, e.getMessage());
            }
            return false;
        }
    }

    @Override
    public int saveNmeaBatch(List<NmeaRecord> records) {
        return saveNmeaBatch(defaultStationId, records);
    }

    @Override
    public int saveNmeaBatch(String stationId, List<NmeaRecord> records) {
        if (!initialized || records == null || records.isEmpty()) {
            return 0;
        }

        try {
            String tableName = getTableName(stationId);
            long currentTime = System.currentTimeMillis();

            // 1. 构造带有占位符的单条 SQL 模板
            String sql = String.format("INSERT INTO %s (ts, nmea_type, raw_content) VALUES (?, ?, ?)", tableName);

            // 2. 将数据组装为批量参数列表
            List<Object[]> batchArgs = new ArrayList<>(records.size());

            for (NmeaRecord record : records) {
                if (record == null || record.getRawContent() == null) {
                    continue;
                }

                long timestamp = record.getReceivedTime() != null ?
                        record.getReceivedTime().getTime() : currentTime;
                String nmeaType = extractNmeaType(record.getRawContent());

                // 直接放入原始字符串，底层驱动的 PreparedStatement 会自动且安全地处理所有特殊字符转义
                batchArgs.add(new Object[]{timestamp, nmeaType, record.getRawContent()});
            }

            if (!batchArgs.isEmpty()) {
                // 3. 执行真正的标准批量插入（底层驱动利用 Batch 技术一次性发往数据库）
                jdbcTemplate.batchUpdate(sql, batchArgs);

                int successCount = batchArgs.size();
                savedCount.addAndGet(successCount);
                batchSavedCount.incrementAndGet();

                return successCount;
            }

            return 0;

        } catch (Exception e) {
            failedCount.addAndGet(records.size());
            logger.error("批量存储 NMEA 数据失败: {}", e.getMessage());
            return 0;
        }
    }

    @Override
    public String getStatistics() {
        return String.format("NMEA[saved=%d, failed=%d, batchOps=%d]",
                savedCount.get(), failedCount.get(), batchSavedCount.get());
    }

    @Override
    public void resetStatistics() {
        savedCount.set(0);
        failedCount.set(0);
        batchSavedCount.set(0);
    }

    // ==================== 私有方法 ====================

    /**
     * 获取表名（支持多站点）
     */
    private String getTableName(String stationId) {
        if (stationId == null || stationId.equals(defaultStationId)) {
            return cachedDefaultTableName;
        }
        return tablePrefix + sanitizeTableName(stationId);
    }

    /**
     * 提取 NMEA 类型
     */
    private String extractNmeaType(String nmea) {
        if (nmea == null || nmea.length() < 6) {
            return "UNKNOWN";
        }
        int start = nmea.startsWith("$") ? 3 : 0;
        if (nmea.length() >= start + 3) {
            return nmea.substring(start, start + 3);
        }
        return "UNKNOWN";
    }

    /**
     * 净化表名
     */
    private String sanitizeTableName(String name) {
        if (name == null) {
            return "unknown";
        }
        return name.replaceAll("[^a-zA-Z0-9_]", "_");
    }

    // 注意：旧版的 escapeString(String str) 方法已被删除，因为使用了 PreparedStatement 后不再需要手动转义。
}