package com.ruoyi.gnss.service.impl;

import com.ruoyi.gnss.domain.GnssSolution;
import com.ruoyi.gnss.service.IGnssStorageService;
import com.ruoyi.user.comm.core.tdengine.TDengineUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.Locale;

/**
 * TDengine GNSS 存储服务实现
 *
 * <p>
 * 修复内容：
 * 1. 使用 TDengineUtil.useDatabase() 替代直接执行 USE 语句
 * 2. 确保数据库存在后再切换
 * </p>
 */
@Service
@ConditionalOnProperty(name = "gnss.tdengine.enabled", havingValue = "true", matchIfMissing = false)
public class TDengineGnssStorageServiceImpl implements IGnssStorageService {

    private static final Logger logger = LoggerFactory.getLogger(TDengineGnssStorageServiceImpl.class);

    @Value("${gnss.tdengine.database:ieims}")
    private String database;

    @Value("${gnss.parser.stationId:8900_1}")
    private String stationId;

    @Value("${gnss.tdengine.tablePrefix:gnss_}")
    private String tablePrefix;

    private static final String STABLE_NAME = "st_gnss_solution";

    @Resource
    private TDengineUtil tdengineUtil;

    private volatile boolean initialized = false;

    @PostConstruct
    public void init() {
        try {
            initTables();
            initialized = true;
            logger.info("TDengine GNSS 存储服务初始化成功 - database: {}", database);
        } catch (Exception e) {
            initialized = false;
            logger.error("TDengine GNSS 存储服务初始化失败: {}", e.getMessage(), e);
        }
    }

    /**
     * 初始化超级表和子表
     *
     * <p>
     * 修复说明：
     * - 使用 tdengineUtil.useDatabase() 替代直接执行 "USE database"
     * - useDatabase() 会先确保数据库存在，然后再切换
     * </p>
     */
    private void initTables() {
// 防止数据库不存在导致建表崩溃，使用兼容性更好的简单SQL替代 tdengineUtil.useDatabase
        try {
            tdengineUtil.executeDDL("CREATE DATABASE IF NOT EXISTS " + database);
        } catch (Exception e) {
            logger.debug("尝试建库失败 (可能已存在或无权限): {}", e.getMessage());
        }

        // 创建超级表
        String createStableSql = String.format(
                "CREATE STABLE IF NOT EXISTS %s (" +
                        "ts TIMESTAMP, lat DOUBLE, lon DOUBLE, alt DOUBLE, " +
                        "status INT, sat_count INT, hdop DOUBLE" +
                        ") TAGS (station_id VARCHAR(50))",
                STABLE_NAME
        );
        tdengineUtil.executeDDL(createStableSql);
        logger.info("超级表已创建/确认: {}.{}", database, STABLE_NAME);

        // 创建默认子表
        String tableName = tablePrefix + sanitizeTableName(stationId);
        String createTableSql = String.format(
                "CREATE TABLE IF NOT EXISTS %s USING %s TAGS ('%s')",
                tableName, STABLE_NAME, stationId
        );
        tdengineUtil.executeDDL(createTableSql);
        logger.info("子表已创建/确认: {}", tableName);
    }

    @Override
    public void saveSolution(String targetStationId, GnssSolution solution) {
        if (!initialized || solution == null) {
            return;
        }

        try {
            String tableName = tablePrefix + sanitizeTableName(targetStationId);
            long timestamp = solution.getTime() != null ? solution.getTime().getTime() : System.currentTimeMillis();

            String insertSql = String.format(Locale.US,
                    "INSERT INTO %s USING %s TAGS ('%s') VALUES (%d, %f, %f, %f, %d, %d, %f)",
                    tableName, STABLE_NAME, targetStationId,
                    timestamp,
                    solution.getLatitude(),
                    solution.getLongitude(),
                    solution.getAltitude(),
                    solution.getStatus(),
                    solution.getSatelliteCount(),
                    solution.getHdop()
            );

            tdengineUtil.executeUpdate(insertSql); // 变成单参数调用

            logger.debug("GNSS 解算结果已存储, 站点: {}", targetStationId);

        } catch (Exception e) {
            logger.error("存储 GNSS 解算结果失败: {}", e.getMessage(), e);
        }
    }

    /**
     * 检查服务是否初始化成功
     */
    public boolean isInitialized() {
        return initialized;
    }

    private String sanitizeTableName(String name) {
        if (name == null) return "unknown";
        return name.replaceAll("[^a-zA-Z0-9_]", "_");
    }
}