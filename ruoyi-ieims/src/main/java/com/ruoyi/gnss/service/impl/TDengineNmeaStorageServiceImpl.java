package com.ruoyi.gnss.service.impl;

import com.ruoyi.gnss.domain.GnssSolution;
import com.ruoyi.gnss.service.IGnssStorageService;
import com.ruoyi.user.comm.core.tdengine.TDengineUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * NMEA 数据存储服务实现类（TDengine 版本）
 *
 * <p>
 * 将 NMEA 解算结果存储到 TDengine 数据库的 st_receiver_status 表中。
 * 数据来源：$GPGGA / $GNGGA 格式的 NMEA 语句
 * </p>
 *
 * <p>
 * 表结构说明：
 * - 超级表：st_receiver_status
 * - 字段：ts（时间戳）、lat（纬度）、lon（经度）、alt（海拔）、sat_used（卫星数）、hdop（精度因子）
 * - 标签：station_id（站点编号）
 * </p>
 *
 * @author GNSS Team
 * @date 2026-03-23
 */
@Service
public class TDengineNmeaStorageServiceImpl implements IGnssStorageService {

    private static final Logger log = LoggerFactory.getLogger(TDengineNmeaStorageServiceImpl.class);

    /** 数据库名称 */
    private static final String DB_NAME = "gnss_test_db";

    /** 默认站点ID */
    private static final String DEFAULT_STATION_ID = "8900_1";

    @Autowired
    private TDengineUtil tdengineUtil;

    @Autowired
    private GnssTDengineService gnssTDengineService;

    /**
     * 保存 GNSS 解算结果到 TDengine
     *
     * @param solution GNSS 解算结果实体
     */
    @Override
    public void saveSolution(GnssSolution solution) {
        if (solution == null) {
            log.warn("⚠️ GNSS 解算结果为空，跳过存储");
            return;
        }

        try {
            // 确保已切换到正确的数据库
            gnssTDengineService.ensureDatabase();

            // 构建子表名称（格式：st_receiver_status_{station_id}）
            String tableName = "st_receiver_status_" + DEFAULT_STATION_ID;

            // 获取时间戳（毫秒）
            long timestamp = solution.getTime() != null ? solution.getTime().getTime() : System.currentTimeMillis();

            // 构建 INSERT SQL
            String insertSql = String.format(
                    "INSERT INTO %s (ts, lat, lon, alt, sat_used, hdop) VALUES (?, ?, ?, ?, ?, ?)",
                    tableName
            );

            // 执行插入（hdop 暂时设为 0，因为 GnssSolution 中没有这个字段）
            int rows = tdengineUtil.executeUpdate(
                    insertSql,
                    timestamp,
                    solution.getLatitude(),
                    solution.getLongitude(),
                    solution.getAltitude(),
                    solution.getSatelliteCount(),
                    0.0  // hdop 默认值
            );

            log.debug("✅ NMEA 数据存储成功: 时间={}, 经度={}, 纬度={}, 高度={}, 卫星数={}, 影响行数={}",
                    timestamp, solution.getLongitude(), solution.getLatitude(),
                    solution.getAltitude(), solution.getSatelliteCount(), rows);

        } catch (Exception e) {
            log.error("❌ NMEA 数据存储失败: {}", e.getMessage(), e);
        }
    }

    /**
     * 保存 NMEA 数据（扩展方法，支持更多字段）
     *
     * @param stationId 站点ID
     * @param timestamp 时间戳（毫秒）
     * @param lat 纬度
     * @param lon 经度
     * @param alt 海拔高度
     * @param satUsed 参与定位的卫星数
     * @param hdop 水平精度因子
     */
    public void saveReceiverStatus(String stationId, long timestamp,
                                   double lat, double lon, double alt, int satUsed, double hdop) {
        try {
            // 确保已切换到正确的数据库
            gnssTDengineService.ensureDatabase();

            // 构建子表名称
            String tableName = "st_receiver_status_" + stationId;

            // 检查子表是否存在，不存在则创建
            createTableIfNotExists(tableName, stationId);

            // 构建 INSERT SQL
            String insertSql = String.format(
                    "INSERT INTO %s (ts, lat, lon, alt, sat_used, hdop) VALUES (?, ?, ?, ?, ?, ?)",
                    tableName
            );

            // 执行插入
            int rows = tdengineUtil.executeUpdate(insertSql, timestamp, lat, lon, alt, satUsed, hdop);

            log.info("✅ 接收机状态存储成功: 站点={}, 时间={}, 经度={:.6f}, 纬度={:.6f}, 高度={:.2f}m, 卫星数={}, HDOP={:.2f}",
                    stationId, timestamp, lon, lat, alt, satUsed, hdop);

        } catch (Exception e) {
            log.error("❌ 接收机状态存储失败: {}", e.getMessage(), e);
        }
    }

    /**
     * 检查并创建子表
     */
    private void createTableIfNotExists(String tableName, String stationId) {
        try {
            String createTableSql = String.format(
                    "CREATE TABLE IF NOT EXISTS %s USING st_receiver_status TAGS ('%s')",
                    tableName, stationId
            );
            tdengineUtil.executeDDL(createTableSql);
        } catch (Exception e) {
            // 表可能已存在，忽略错误
            log.debug("子表 {} 可能已存在: {}", tableName, e.getMessage());
        }
    }
}
