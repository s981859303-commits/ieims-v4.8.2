package com.ruoyi.gnss.service.impl;

import com.ruoyi.user.comm.core.tdengine.TDengineUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * RTCM 观测数据存储服务实现类（TDengine 版本）
 *
 * <p>
 * 将 RTCM 1074/1127 解算出的卫星观测数据存储到 TDengine 数据库的 st_sat_obs 表中。
 * 数据来源：RTCM 1074（GPS MSM4）、RTCM 1127（BeiDou MSM4）等观测值消息
 * </p>
 *
 * <p>
 * 表结构说明：
 * - 超级表：st_sat_obs
 * - 字段：ts、elevation、azimuth、c1、snr1、p1、l1、c2、snr2、p2、l2
 * - 标签：station_id、sat_name
 * </p>
 *
 * @author GNSS Team
 * @date 2026-03-23
 */
@Service
public class TDengineRtcmStorageServiceImpl {

    private static final Logger log = LoggerFactory.getLogger(TDengineRtcmStorageServiceImpl.class);

    /** 数据库名称 */
    private static final String DB_NAME = "gnss_test_db";

    /** 默认站点ID */
    private static final String DEFAULT_STATION_ID = "8900_1";

    /** 已创建的子表缓存（避免重复创建） */
    private static final Map<String, Boolean> createdTables = new ConcurrentHashMap<>();

    @Autowired
    private TDengineUtil tdengineUtil;

    @Autowired
    private GnssTDengineService gnssTDengineService;

    /**
     * 卫星观测数据实体类
     */
    public static class SatObsData {
        public String satName;      // 卫星编号（如 G01, C02）
        public double elevation;    // 仰角（度）
        public double azimuth;      // 方位角（度）
        public String c1;           // L1 信号类型代码
        public double snr1;         // L1 信噪比
        public double p1;           // L1 伪距（米）
        public double l1;           // L1 载波相位（周）
        public String c2;           // L2 信号类型代码
        public double snr2;         // L2 信噪比
        public double p2;           // L2 伪距（米）
        public double l2;           // L2 载波相位（周）
    }

    /**
     * 保存单颗卫星的观测数据
     *
     * @param stationId 站点ID
     * @param timestamp 时间戳（毫秒）
     * @param obsData 卫星观测数据
     */
    public void saveSatObsData(String stationId, long timestamp, SatObsData obsData) {
        if (obsData == null || obsData.satName == null || obsData.satName.isEmpty()) {
            return;
        }

        try {
            // 确保已切换到正确的数据库
            gnssTDengineService.ensureDatabase();

            // 构建子表名称（格式：st_sat_obs_{station_id}_{sat_name}）
            String tableName = String.format("st_sat_obs_%s_%s", stationId, obsData.satName);

            // 检查并创建子表
            createTableIfNotExists(tableName, stationId, obsData.satName);

            // 构建 INSERT SQL
            String insertSql = String.format(
                    "INSERT INTO %s (ts, elevation, azimuth, c1, snr1, p1, l1, c2, snr2, p2, l2) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    tableName
            );

            // 执行插入
            int rows = tdengineUtil.executeUpdate(
                    insertSql,
                    timestamp,
                    obsData.elevation,
                    obsData.azimuth,
                    obsData.c1 != null ? obsData.c1 : "",
                    obsData.snr1,
                    obsData.p1,
                    obsData.l1,
                    obsData.c2 != null ? obsData.c2 : "",
                    obsData.snr2,
                    obsData.p2,
                    obsData.l2
            );

            log.debug("✅ 卫星观测数据存储成功: 卫星={}, 时间={}, P1={:.3f}, L1={:.3f}, SNR1={:.1f}",
                    obsData.satName, timestamp, obsData.p1, obsData.l1, obsData.snr1);

        } catch (Exception e) {
            log.error("❌ 卫星观测数据存储失败: 卫星={}, 错误={}",
                    obsData != null ? obsData.satName : "null", e.getMessage());
        }
    }

    /**
     * 批量保存多颗卫星的观测数据
     *
     * @param stationId 站点ID
     * @param timestamp 时间戳（毫秒）
     * @param obsDataList 卫星观测数据列表
     */
    public void saveSatObsDataBatch(String stationId, long timestamp, List<SatObsData> obsDataList) {
        if (obsDataList == null || obsDataList.isEmpty()) {
            return;
        }

        // 确保已切换到正确的数据库
        gnssTDengineService.ensureDatabase();

        int successCount = 0;
        int failCount = 0;

        for (SatObsData obsData : obsDataList) {
            try {
                saveSatObsData(stationId, timestamp, obsData);
                successCount++;
            } catch (Exception e) {
                failCount++;
                log.warn("⚠️ 卫星 {} 数据存储失败: {}", obsData.satName, e.getMessage());
            }
        }

        if (successCount > 0) {
            log.info("✅ 批量存储卫星观测数据完成: 成功={}, 失败={}, 时间={}",
                    successCount, failCount, timestamp);
        }
    }

    /**
     * 检查并创建子表
     */
    private void createTableIfNotExists(String tableName, String stationId, String satName) {
        // 检查缓存
        if (createdTables.containsKey(tableName)) {
            return;
        }

        try {
            String createTableSql = String.format(
                    "CREATE TABLE IF NOT EXISTS %s USING st_sat_obs TAGS ('%s', '%s')",
                    tableName, stationId, satName
            );
            tdengineUtil.executeDDL(createTableSql);
            createdTables.put(tableName, true);
            log.debug("✅ 创建子表成功: {}", tableName);
        } catch (Exception e) {
            // 表可能已存在，忽略错误
            log.debug("子表 {} 可能已存在: {}", tableName, e.getMessage());
            createdTables.put(tableName, true);
        }
    }

    /**
     * 清除子表缓存（用于重新初始化）
     */
    public void clearTableCache() {
        createdTables.clear();
        log.info("✅ 已清除子表缓存");
    }

    /**
     * 查询指定卫星的观测数据
     *
     * @param stationId 站点ID
     * @param satName 卫星编号
     * @param startTime 开始时间戳（毫秒）
     * @param endTime 结束时间戳（毫秒）
     * @return 观测数据列表
     */
    public List<Map<String, Object>> querySatObsData(String stationId, String satName,
                                                     long startTime, long endTime) {
        try {
            gnssTDengineService.ensureDatabase();

            String tableName = String.format("st_sat_obs_%s_%s", stationId, satName);
            String querySql = String.format(
                    "SELECT ts, elevation, azimuth, c1, snr1, p1, l1, c2, snr2, p2, l2 " +
                            "FROM %s WHERE ts >= ? AND ts <= ? ORDER BY ts",
                    tableName
            );

            return tdengineUtil.queryForList(querySql, startTime, endTime);

        } catch (Exception e) {
            log.error("❌ 查询卫星观测数据失败: {}", e.getMessage(), e);
            return java.util.Collections.emptyList();
        }
    }
}
