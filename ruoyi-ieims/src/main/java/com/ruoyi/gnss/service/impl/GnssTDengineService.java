package com.ruoyi.gnss.service.impl;

import com.ruoyi.user.comm.core.tdengine.TDengineUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;

/**
 * GNSS TDengine 数据库服务
 *
 * <p>
 * 负责初始化 TDengine 数据库表结构，包括：
 * 1. 创建数据库 gnss_test_db
 * 2. 创建超级表 st_receiver_status（接收机基础状态表）
 * 3. 创建超级表 st_sat_obs（卫星原始观测数据表）
 * 4. 创建对应的子表
 * </p>
 *
 * @author GNSS Team
 * @date 2026-03-23
 */
@Service
public class GnssTDengineService {

    private static final Logger log = LoggerFactory.getLogger(GnssTDengineService.class);

    /** 数据库名称 */
    private static final String DB_NAME = "gnss_test_db";

    /** 默认站点ID */
    private static final String DEFAULT_STATION_ID = "8900_1";

    @Resource
    private TDengineUtil tdengineUtil;

    /**
     * 初始化 TDengine 表结构
     * 应用启动时自动执行
     */
    @PostConstruct
    public void initTDengineTables() {
        try {
            log.info("========== 开始初始化 GNSS TDengine 表结构 ==========");

            // 1. 创建数据库（保留10年数据）
            String createDbSql = String.format(
                    "CREATE DATABASE IF NOT EXISTS %s KEEP 3650",
                    DB_NAME
            );
            tdengineUtil.executeDDL(createDbSql);
            log.info("✅ 创建数据库成功: {}", createDbSql);

            // 2. 切换到目标数据库
            tdengineUtil.executeDDL("USE " + DB_NAME);
            log.info("✅ 切换到数据库: {}", DB_NAME);

            // 3. 创建超级表：接收机基础状态表
            String createReceiverStatusStable =
                    "CREATE STABLE IF NOT EXISTS st_receiver_status (" +
                            "    ts TIMESTAMP, " +           // 时间戳
                            "    lat DOUBLE, " +             // 纬度
                            "    lon DOUBLE, " +             // 经度
                            "    alt DOUBLE, " +             // 海拔高度（米）
                            "    sat_used INT, " +           // 参与定位的卫星数
                            "    hdop DOUBLE " +             // 水平精度因子
                            ") TAGS (" +
                            "    station_id VARCHAR(20)" +   // 标签：站点编号
                            ")";
            tdengineUtil.executeDDL(createReceiverStatusStable);
            log.info("✅ 创建超级表成功: st_receiver_status");

            // 4. 创建超级表：卫星原始观测数据表
            String createSatObsStable =
                    "CREATE STABLE IF NOT EXISTS st_sat_obs (" +
                            "    ts TIMESTAMP, " +           // 时间戳
                            "    elevation DOUBLE, " +       // 卫星仰角（度）
                            "    azimuth DOUBLE, " +         // 卫星方位角（度）
                            // L1 / B1 频点数据
                            "    c1 VARCHAR(8), " +          // L1 信号类型代码
                            "    snr1 DOUBLE, " +            // L1 信噪比
                            "    p1 DOUBLE, " +              // L1 伪距（米）
                            "    l1 DOUBLE, " +              // L1 载波相位（周）
                            // L2 / B2 频点数据
                            "    c2 VARCHAR(8), " +          // L2 信号类型代码
                            "    snr2 DOUBLE, " +            // L2 信噪比
                            "    p2 DOUBLE, " +              // L2 伪距（米）
                            "    l2 DOUBLE " +               // L2 载波相位（周）
                            ") TAGS (" +
                            "    station_id VARCHAR(20), " + // 标签：站点编号
                            "    sat_name VARCHAR(10)" +     // 标签：卫星编号
                            ")";
            tdengineUtil.executeDDL(createSatObsStable);
            log.info("✅ 创建超级表成功: st_sat_obs");

            // 5. 创建默认子表：接收机状态表
            String createReceiverTable = String.format(
                    "CREATE TABLE IF NOT EXISTS st_receiver_status_%s " +
                            "USING st_receiver_status TAGS ('%s')",
                    DEFAULT_STATION_ID, DEFAULT_STATION_ID
            );
            tdengineUtil.executeDDL(createReceiverTable);
            log.info("✅ 创建子表成功: st_receiver_status_{}", DEFAULT_STATION_ID);

            log.info("========== GNSS TDengine 表结构初始化完成 ==========");

        } catch (Exception e) {
            log.error("❌ GNSS TDengine 表结构初始化失败", e);
            // 不抛出异常，允许应用继续启动
        }
    }

    /**
     * 获取数据库名称
     */
    public String getDbName() {
        return DB_NAME;
    }

    /**
     * 获取默认站点ID
     */
    public String getDefaultStationId() {
        return DEFAULT_STATION_ID;
    }

    /**
     * 确保已切换到正确的数据库
     */
    public void ensureDatabase() {
        try {
            tdengineUtil.executeDDL("USE " + DB_NAME);
        } catch (Exception e) {
            log.warn("切换数据库失败: {}", e.getMessage());
        }
    }

    /**
     * 测试数据库连接
     * @return 服务器版本信息
     */
    public String testConnection() {
        try {
            String version = tdengineUtil.queryForObject("SELECT SERVER_VERSION()", String.class);
            log.info("✅ TDengine 连接成功，版本: {}", version);
            return version;
        } catch (Exception e) {
            log.error("❌ TDengine 连接测试失败", e);
            return null;
        }
    }
}

