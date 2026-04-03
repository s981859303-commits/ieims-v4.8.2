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
 * 5. 使用 JdbcTemplate 的批量 PreparedStatement 替代字符串拼接，彻底杜绝 SQL 注入并大幅提升批量写入性能
 * </p>
 *
 * <p>
 * v2.1 修复内容：
 * 1. 使用 TDengineUtil.useDatabase() 替代直接执行 USE 语句
 * 2. 确保数据库存在后再切换
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

    @Resource
    private JdbcTemplate jdbcTemplate;

    // ==================== 缓存变量 ====================

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
            logger.info("TDengine NMEA 存储服务初始化成功 - database: {}, 默认站点: {}, 默认表: {}",
                    database, defaultStationId, cachedDefaultTableName);
        } catch (Exception e) {
            initialized = false;
            logger.error("TDengine NMEA 存储服务初始化失败: {}", e.getMessage(), e);
        }
    }

    /**
     * 初始化超级表和默认子表
     *
     * <p>
     * 修复说明：
     * - 使用 tdengineUtil.useDatabase() 替代直接执行 "USE database"
     * - useDatabase() 会先确保数据库存在，然后再切换
     * </p>
     */
    private void initTables() {
        // 【修复】使用 useDatabase 替代直接执行 USE 语句
        tdengineUtil.useDatabase(database);

        // 创建超级表
        String createStableSql = String.format(
                "CREATE STABLE IF NOT EXISTS %s (" +
                        "ts TIMESTAMP, nmea_type VARCHAR(10), raw_content NCHAR(512)" +
                        ") TAGS (station_id VARCHAR(50))",
                STABLE_NAME
        );
        tdengineUtil.executeDDL(createStableSql);
        logger.info("超级表已创建/确认: {}.{}", database, STABLE_NAME);

        // 创建默认子表
        String createTableSql = String.format(
                "CREATE TABLE IF NOT EXISTS %s USING %s TAGS ('%s')",
                cachedDefaultTableName, STABLE_NAME, defaultStationId
        );
        tdengineUtil.executeDDL(createTableSql);
        logger.info("子表已创建/确认: {}", cachedDefaultTableName);
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

            String sql = String.format("INSERT INTO %s (ts, nmea_type, raw_content) VALUES (?, ?, ?)", tableName);

            List<Object[]> batchArgs = new ArrayList<>(records.size());

            for (NmeaRecord record : records) {
                if (record == null || record.getRawContent() == null) {
                    continue;
                }

                long timestamp = record.getReceivedTime() != null ?
                        record.getReceivedTime().getTime() : currentTime;
                String nmeaType = extractNmeaType(record.getRawContent());

                batchArgs.add(new Object[]{timestamp, nmeaType, record.getRawContent()});
            }

            if (!batchArgs.isEmpty()) {
                jdbcTemplate.batchUpdate(sql, batchArgs);

                int successCount = batchArgs.size();
                savedCount.addAndGet(successCount);
                batchSavedCount.incrementAndGet();

                return successCount;
            }

            return 0;

        } catch (Exception e) {
            failedCount.addAndGet(records.size());
            logger.error("批量存储 NMEA 数据失败: {}", e.getMessage(), e);
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
}