package com.ruoyi.gnss.service.impl;

import com.ruoyi.gnss.domain.NmeaRecord;
import com.ruoyi.gnss.service.INmeaStorageService;
import com.ruoyi.user.comm.core.tdengine.TDengineUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
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
 * 4. 支持批量写入
 * 5. 支持多站点
 * </p>
 *
 * @author GNSS Team
 * @date 2026-03-26
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

            String insertSql = String.format(
                    "INSERT INTO %s (ts, nmea_type, raw_content) VALUES (?, ?, ?)",
                    tableName
            );

            tdengineUtil.executeUpdate(insertSql, timestamp, nmeaType, record.getRawContent());
            savedCount.incrementAndGet();

            // 降低日志频率
            if (logger.isTraceEnabled()) {
                logger.trace("NMEA 数据已存储: {}", nmeaType);
            }
            return true;

        } catch (Exception e) {
            failedCount.incrementAndGet();
            // 每100次失败打印一次日志，避免日志刷屏
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

            // 预估 SQL 长度：每条约 150 字节
            int estimatedLength = 20 + records.size() * 150;
            StringBuilder sqlBuilder = new StringBuilder(estimatedLength);
            sqlBuilder.append("INSERT INTO ");

            int successCount = 0;
            long currentTime = System.currentTimeMillis();

            for (NmeaRecord record : records) {
                if (record == null || record.getRawContent() == null) {
                    continue;
                }

                long timestamp = record.getReceivedTime() != null ?
                        record.getReceivedTime().getTime() : currentTime;
                String nmeaType = extractNmeaType(record.getRawContent());

                // 构建 VALUES 子句
                sqlBuilder.append(tableName)
                        .append(" VALUES (")
                        .append(timestamp)
                        .append(", '")
                        .append(nmeaType)
                        .append("', '")
                        .append(escapeString(record.getRawContent()))
                        .append("') ");

                successCount++;
            }

            if (successCount > 0) {
                tdengineUtil.executeUpdate(sqlBuilder.toString());
                savedCount.addAndGet(successCount);
                batchSavedCount.incrementAndGet();
            }

            return successCount;

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
        // 非默认站点，动态计算表名
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

    /**
     * 转义字符串中的特殊字符
     */
    private String escapeString(String str) {
        if (str == null) {
            return "";
        }
        // 转义单引号
        return str.replace("'", "''");
    }
}
