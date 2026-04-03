package com.ruoyi.gnss.service;

import com.ruoyi.gnss.domain.SatObservation;

import java.time.LocalDate;
import java.util.List;

/**
 * 卫星观测数据存储服务接口（修复版）
 *
 *  * 功能：
 *  * 1. 保存卫星观测数据到 TDengine
 *  * 2. 按时间范围查询观测数据
 *  * 3. 按日期查询观测数据
 *  * 4. 按唯一键查询观测数据
 *  * 5. 删除指定时间范围的数据
 *  * 6. 获取统计信息
 *
 * @version 2.1 - 2026-04-02 添加重载方法
 */
public interface ISatObservationStorageService {

    /**
     * 保存单条观测数据
     *
     * @param stationId 站点ID
     * @param observation 观测数据
     */
    void saveSatObservation(String stationId, SatObservation observation);

    /**
     * 批量保存观测数据
     *
     * @param stationId 站点ID
     * @param observations 观测数据列表
     */
    void saveSatObservationBatch(String stationId, List<SatObservation> observations);

    /**
     * 【新增】批量保存观测数据（无需站点ID）
     *
     * 新增说明：
     * - GnssAsyncProcessor 需要调用此方法
     * - 当观测数据已包含 stationId 时使用
     *
     * @param observations 观测数据列表
     */
    default void saveSatObservationBatch(List<SatObservation> observations) {
        if (observations == null || observations.isEmpty()) {
            return;
        }
        // 默认实现：从第一条数据获取站点ID
        String stationId = observations.get(0).getStationId();
        if (stationId == null || stationId.isEmpty()) {
            stationId = "default";
        }
        saveSatObservationBatch(stationId, observations);
    }

    /**
     * 按时间范围查询
     *
     * @param stationId 站点ID
     * @param startTime 开始时间（毫秒）
     * @param endTime 结束时间（毫秒）
     * @return 观测数据列表
     */
    List<SatObservation> queryByTimeRange(String stationId, long startTime, long endTime);

    /**
     * 按日期查询
     *
     * @param stationId 站点ID
     * @param date 日期
     * @return 观测数据列表
     */
    List<SatObservation> queryByDate(String stationId, LocalDate date);

    /**
     * 按唯一键查询
     *
     * @param stationId 站点ID
     * @param obsUniqueKey 唯一键
     * @return 观测数据
     */
    SatObservation queryByUniqueKey(String stationId, String obsUniqueKey);

    /**
     * 按时间范围删除
     *
     * @param stationId 站点ID
     * @param startTime 开始时间（毫秒）
     * @param endTime 结束时间（毫秒）
     * @return 删除数量
     */
    int deleteByTimeRange(String stationId, long startTime, long endTime);

    /**
     * 获取统计信息
     *
     * @return 统计信息字符串
     */
    String getStatistics();
}
