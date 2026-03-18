package com.ruoyi.test;

import com.ruoyi.user.comm.core.tdengine.TDengineUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.util.List;
import java.util.Map;

@Service
public class IonosphereDataService {
    private static final Logger log = LoggerFactory.getLogger(IonosphereDataService.class);

    @Resource
    private TDengineUtil tdengineUtil;

    /**
     * 初始化 TDengine 表（超级表+子表）- 适配 TDengine 3.x 语法
     */
    @PostConstruct
    public void initTDengineTable() {
        try {
//            {
//                tdengineUtil.executeDDL("use ieims");
//                //删除子表（仅删子表，超级表和其他子表不受影响）
//                String sql="DROP TABLE IF EXISTS device_01";
//                tdengineUtil.executeDDL(sql);
//                log.info("✅ 执行SQL成功：{}", sql);
//                //删除超级表（会同时删除所有子表，谨慎操作）
//                sql="DROP STABLE IF EXISTS device_data";
//                tdengineUtil.executeDDL(sql);
//                log.info("✅ 执行SQL成功：{}", sql);
//            }

            // 1. 创建数据库并指定字符集/时区（3.x 推荐配置）
            String createDbSql = "CREATE DATABASE IF NOT EXISTS ieims";
            tdengineUtil.executeDDL(createDbSql);
            log.info("✅ 执行SQL成功：{}", createDbSql);

            // 2. 必须先切换到目标数据库，否则表会创建到默认数据库
            tdengineUtil.executeDDL("USE ieims");
            log.info("✅ 切换到ieims数据库成功");

            // 3. 创建超级表（列名统一用小写，避免大小写兼容问题）  // ts时间戳（TDengine保留字段，必须小写）
            String createStableSql = "CREATE STABLE IF NOT EXISTS device_data (ts TIMESTAMP, signal_strength FLOAT, " +
                    "electron_density INT, temperature FLOAT) " +
                    "TAGS (station_id INT, region VARCHAR(20), device_type VARCHAR(50))";
            tdengineUtil.executeDDL(createStableSql);
            log.info("✅ 执行SQL成功：{}", createStableSql);

            // 4. 创建子表（已切换数据库，无需带库名前缀）
            String createTableSql ="CREATE TABLE IF NOT EXISTS device_01  USING device_data TAGS (1, '华北', '电离层监测')";
            tdengineUtil.executeDDL(createTableSql);
            log.info("✅ 执行SQL成功：{}", createTableSql);

            log.info("🎉 TDengine 表初始化完成！");
        } catch (Exception e) {
            log.error("❌ TDengine 表初始化失败", e);
            throw new RuntimeException("TDengine 表初始化失败", e);
        }
    }

    /**
     * 插入电离层数据（包含温度字段）
     * @param stationId 站点ID
     * @param region 区域
     * @param ts 时间戳（毫秒）
     * @param signalStrength 信号强度
     * @param electronDensity 电子密度
     * @param temperature 温度（可为null）
     */
    public void insertIonDataWithTemperature(int stationId, String region, long ts, float signalStrength, int electronDensity, Float temperature) {
        String insertSql;
        if (temperature == null) {
            insertSql = "INSERT INTO device_01 (ts, signal_strength, electron_density, temperature) VALUES (?, ?, ?, NULL)";
        } else {
            insertSql = "INSERT INTO device_01 (ts, signal_strength, electron_density, temperature) VALUES (?, ?, ?, ?)";
        }

        try {
//            tdengineUtil.executeDDL("USE ieims");

            if (temperature == null) {
                tdengineUtil.executeUpdate(insertSql, ts, signalStrength, electronDensity);
            } else {
                tdengineUtil.executeUpdate(insertSql, ts, signalStrength, electronDensity, temperature);
            }
//            log.info("✅ 插入数据成功，SQL：{}，参数：[{}, {}, {}, {}]", insertSql, ts, signalStrength, electronDensity, temperature);
        } catch (Exception e) {
            log.error("❌ 插入电离层数据（含温度）失败", e);
            throw new RuntimeException("TDengine执行DML失败：" + insertSql, e);
        }
    }

    /**
     * 查询电离层数据
     * @param startTime 开始时间戳（毫秒）
     * @param endTime 结束时间戳（毫秒）
     * @return 数据列表
     */
    public List<Map<String, Object>> queryIonData(long startTime, long endTime) {
        String querySql = "SELECT ts, signal_strength, electron_density FROM device_01 WHERE ts >= ? AND ts <= ?";
        try {
            // 先切换数据库
//            tdengineUtil.executeDDL("USE ieims");

            log.info("执行查询SQL：{}，参数：[{}, {}]", querySql, startTime, endTime);
            return tdengineUtil.queryForList(querySql, startTime, endTime);
        } catch (Exception e) {
            log.error("❌ 查询电离层数据失败", e);
            throw new RuntimeException("TDengine执行查询失败：" + querySql, e);
        }
    }

    /**
     * 测试TDengine连接
     * @return 服务器版本
     */
    public String testConnection() {
        try {
            String sql = "SELECT SERVER_VERSION()";
            log.info("执行版本查询SQL：{}", sql);
            String version = tdengineUtil.queryForObject(sql, String.class);
            log.info("✅ TDengine连接成功，版本：{}", version);
            return version;
        } catch (Exception e) {
            log.error("❌ TDengine连接测试失败", e);
            throw new RuntimeException("TDengine连接测试失败", e);
        }
    }
}