package com.ruoyi.gnss.service;

import com.ruoyi.gnss.domain.SatObservation;

import java.time.LocalDate;
import java.util.List;

/**
 * 卫星观测数据存储服务接口（增强版 - 支持日期字段）
 *
 * 功能：
 * 1. 保存卫星观测数据到 TDengine
 * 2. 按时间范围查询观测数据
 * 3. 按日期查询观测数据
 * 4. 按唯一键查询观测数据
 * 5. 删除指定时间范围的数据
 * 6. 获取统计信息
 *
 * 【重构说明】
 * 1. 新增按日期查询方法
 * 2. 新增按唯一键查询方法
 * 3. 支持批量插入优化
 *
 * @author GNSS Team
 * @version 2.0 - 2026-04-02 添加日期查询支持
 */
public interface ISatObservationStorageService {

    /**
     * 保存单条卫星观测数据
     *
     * @param stationId   站点ID
     * @param observation 观测数据
     */
    void saveSatObservation(String stationId, SatObservation observation);

    /**
     * 批量保存卫星观测数据
     *
     * @param stationId    站点ID
     * @param observations 观测数据列表
     */
    void saveSatObservationBatch(String stationId, List<SatObservation> observations);

    /**
     * 按时间范围查询观测数据
     *
     * @param stationId 站点ID
     * @param startTime 开始时间（毫秒时间戳）
     * @param endTime   结束时间（毫秒时间戳）
     * @return 观测数据列表
     */
    List<SatObservation> queryByTimeRange(String stationId, long startTime, long endTime);

    /**
     * 按日期查询观测数据
     *
     * @param stationId 站点ID
     * @param date      观测日期
     * @return 观测数据列表
     */
    List<SatObservation> queryByDate(String stationId, LocalDate date);

    /**
     * 按唯一键查询观测数据
     *
     * @param stationId    站点ID
     * @param obsUniqueKey 唯一键（格式：日期_时间_卫星编号）
     * @return 观测数据，不存在返回 null
     */
    SatObservation queryByUniqueKey(String stationId, String obsUniqueKey);

    /**
     * 删除指定时间范围的数据
     *
     * @param stationId 站点ID
     * @param startTime 开始时间（毫秒时间戳）
     * @param endTime   结束时间（毫秒时间戳）
     * @return 删除条数
     */
    int deleteByTimeRange(String stationId, long startTime, long endTime);

    /**
     * 获取统计信息
     *
     * @return 统计信息字符串
     */
    String getStatistics();
}
