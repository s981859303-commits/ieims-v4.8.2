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

/**
 * TDengine GNSS 存储服务实现
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

    private boolean initialized = false;

    @PostConstruct
    public void init() {
        try {
            initTables();
            initialized = true;
            logger.info("TDengine GNSS 存储服务初始化成功");
        } catch (Exception e) {
            logger.error("TDengine GNSS 存储服务初始化失败: {}", e.getMessage());
        }
    }

    private void initTables() {
        tdengineUtil.executeDDL("USE " + database);

        String createStableSql = String.format(
            "CREATE STABLE IF NOT EXISTS %s (" +
            "ts TIMESTAMP, lat DOUBLE, lon DOUBLE, alt DOUBLE, " +
            "status INT, sat_count INT, hdop DOUBLE" +
            ") TAGS (station_id VARCHAR(50))",
            STABLE_NAME
        );
        tdengineUtil.executeDDL(createStableSql);

        String tableName = tablePrefix + sanitizeTableName(stationId);
        String createTableSql = String.format(
            "CREATE TABLE IF NOT EXISTS %s USING %s TAGS ('%s')",
            tableName, STABLE_NAME, stationId
        );
        tdengineUtil.executeDDL(createTableSql);
        
        logger.info("创建 GNSS 表: {}", tableName);
    }

    @Override
    public void saveSolution(GnssSolution solution) {
        if (!initialized || solution == null) return;

        try {
            String tableName = tablePrefix + sanitizeTableName(stationId);
            long timestamp = solution.getTime() != null ? solution.getTime().getTime() : System.currentTimeMillis();

            String insertSql = String.format(
                "INSERT INTO %s (ts, lat, lon, alt, status, sat_count, hdop) VALUES (?, ?, ?, ?, ?, ?, ?)",
                tableName
            );

            tdengineUtil.executeUpdate(insertSql,
                timestamp,
                solution.getLatitude(),
                solution.getLongitude(),
                solution.getAltitude(),
                solution.getStatus(),
                solution.getSatelliteCount(),
                solution.getHdop()
            );

            logger.debug("GNSS 解算结果已存储");

        } catch (Exception e) {
            logger.error("存储 GNSS 解算结果失败: {}", e.getMessage());
        }
    }

    private String sanitizeTableName(String name) {
        if (name == null) return "unknown";
        return name.replaceAll("[^a-zA-Z0-9_]", "_");
    }
}
