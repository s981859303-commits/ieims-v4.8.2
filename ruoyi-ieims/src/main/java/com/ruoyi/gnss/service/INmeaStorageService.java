package com.ruoyi.gnss.service;

import com.ruoyi.gnss.domain.NmeaRecord;

import java.util.List;

/**
 * NMEA 数据存储接口
 *
 * <p>
 * 功能说明：
 * 1. 定义 NMEA 数据存储的标准接口
 * 2. 支持多种存储后端实现（TDengine、InfluxDB 等）
 * 3. 遵循依赖倒置原则
 * </p>
 *
 * @author GNSS Team
 * @date 2026-03-26
 */
public interface INmeaStorageService {

    /**
     * 检查服务是否已初始化
     *
     * @return true 表示已初始化
     */
    default boolean isInitialized() {
        return true;
    }

    /**
     * 保存单条 NMEA 数据
     *
     * @param record 封装好的 NMEA 数据对象
     * @return 是否保存成功
     */
    boolean saveNmeaData(NmeaRecord record);

    /**
     * 保存单条 NMEA 数据（指定站点）
     *
     * @param stationId 站点ID
     * @param record    NMEA 数据对象
     * @return 是否保存成功
     */
    default boolean saveNmeaData(String stationId, NmeaRecord record) {
        // 默认实现：忽略 stationId，调用单参数方法
        return saveNmeaData(record);
    }

    /**
     * 批量保存 NMEA 数据
     *
     * @param records NMEA 数据列表
     * @return 成功保存的数量
     */
    default int saveNmeaBatch(List<NmeaRecord> records) {
        int count = 0;
        for (NmeaRecord record : records) {
            if (saveNmeaData(record)) {
                count++;
            }
        }
        return count;
    }

    /**
     * 批量保存 NMEA 数据（指定站点）
     *
     * @param stationId 站点ID
     * @param records   NMEA 数据列表
     * @return 成功保存的数量
     */
    default int saveNmeaBatch(String stationId, List<NmeaRecord> records) {
        int count = 0;
        for (NmeaRecord record : records) {
            if (saveNmeaData(stationId, record)) {
                count++;
            }
        }
        return count;
    }

    /**
     * 获取存储统计信息
     *
     * @return 统计信息字符串
     */
    default String getStatistics() {
        return "Not implemented";
    }

    /**
     * 获取待处理数据数量
     *
     * @return 待处理数据数量
     */
    default int getPendingCount() {
        return 0;
    }

    /**
     * 重置统计信息
     */
    default void resetStatistics() {
        // 默认空实现
    }
}
