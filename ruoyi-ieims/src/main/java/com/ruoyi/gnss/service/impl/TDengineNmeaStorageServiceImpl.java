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

/**
 * TDengine NMEA 存储服务实现
 */
@Service
@ConditionalOnProperty(name = "gnss.tdengine.enabled", havingValue = "true", matchIfMissing = false)
public class TDengineNmeaStorageServiceImpl implements INmeaStorageService {

    private static final Logger logger = LoggerFactory.getLogger(TDengineNmeaStorageServiceImpl.class);

    @Value("${gnss.tdengine.database:ieims}")
    private String database;

    @Value("${gnss.parser.stationId:8900_1}")
    private String stationId;

    @Value("${gnss.tdengine.tablePrefix:nmea_}")
    private String tablePrefix;

    private static final String STABLE_NAME = "st_nmea_raw";

    @Resource
    private TDengineUtil tdengineUtil;

    private boolean initialized = false;

    @PostConstruct
    public void init() {
        try {
            initTables();
            initialized = true;
            logger.info("TDengine NMEA 存储服务初始化成功");
        } catch (Exception e) {
            logger.error("TDengine NMEA 存储服务初始化失败: {}", e.getMessage());
        }
    }

    private void initTables() {
        tdengineUtil.executeDDL("USE " + database);

        String createStableSql = String.format(
                "CREATE STABLE IF NOT EXISTS %s (" +
                        "ts TIMESTAMP, nmea_type VARCHAR(10), raw_content NCHAR(512)" +
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

        logger.info("创建 NMEA 表: {}", tableName);
    }

    @Override
    public boolean saveNmeaData(NmeaRecord record) {
        if (!initialized || record == null || record.getRawContent() == null) return false;

        try {
            String tableName = tablePrefix + sanitizeTableName(stationId);
            long timestamp = record.getReceivedTime() != null ?
                    record.getReceivedTime().getTime() : System.currentTimeMillis();

            String nmeaType = extractNmeaType(record.getRawContent());

            String insertSql = String.format(
                    "INSERT INTO %s (ts, nmea_type, raw_content) VALUES (?, ?, ?)",
                    tableName
            );

            tdengineUtil.executeUpdate(insertSql, timestamp, nmeaType, record.getRawContent());
            logger.debug("NMEA 数据已存储: {}", nmeaType);
            return true;

        } catch (Exception e) {
            logger.error("存储 NMEA 数据失败: {}", e.getMessage());
            return false;
        }
    }

    private String extractNmeaType(String nmea) {
        if (nmea == null || nmea.length() < 6) return "UNKNOWN";
        int start = nmea.startsWith("$") ? 3 : 0;
        if (nmea.length() >= start + 3) {
            return nmea.substring(start, start + 3);
        }
        return "UNKNOWN";
    }

    private String sanitizeTableName(String name) {
        if (name == null) return "unknown";
        return name.replaceAll("[^a-zA-Z0-9_]", "_");
    }
}
