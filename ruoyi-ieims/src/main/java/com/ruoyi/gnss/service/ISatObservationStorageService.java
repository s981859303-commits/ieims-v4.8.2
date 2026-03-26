package com.ruoyi.gnss.service;

import com.ruoyi.gnss.domain.SatObservation;

import java.util.List;

/**
 * 卫星观测数据存储服务接口
 *
 * 功能说明：
 * 1. 定义卫星观测数据存储的标准接口
 * 2. 支持多种存储后端实现（TDengine、InfluxDB 等）
 * 3. 遵循依赖倒置原则
 *
 * @author GNSS Team
 * @date 2026-03-25
 */
public interface ISatObservationStorageService {

    /**
     * 检查服务是否已初始化
     *
     * @return true 表示已初始化
     */
    boolean isInitialized();

    /**
     * 保存单条卫星观测数据
     *
     * @param observation 卫星观测数据
     * @return true 表示保存成功
     */
    boolean saveSatObservation(SatObservation observation);

    /**
     * 批量保存卫星观测数据
     *
     * @param observations 卫星观测数据列表
     * @return 成功保存的数量
     */
    int saveSatObservationBatch(List<SatObservation> observations);

    /**
     * 批量保存卫星观测数据（指定站点）
     *
     * @param stationId 站点ID
     * @param observations 卫星观测数据列表
     * @return 成功保存的数量
     */
    int saveSatObservationBatch(String stationId, List<SatObservation> observations);

    /**
     * 异步批量保存卫星观测数据
     *
     * @param observations 卫星观测数据列表
     */
    default void saveSatObservationBatchAsync(List<SatObservation> observations) {
        // 默认实现：同步调用
        saveSatObservationBatch(observations);
    }

    /**
     * 获取待处理数据数量
     *
     * @return 待处理数据数量
     */
    default int getPendingCount() {
        return 0;
    }
}
