package com.ruoyi.ieims.gnss.service;

import com.ruoyi.ieims.util.PhaseUtil;
import com.ruoyi.ieims.gnss.domain.PhaseScintillation;

import java.util.List;

/**
 * 相位闪烁指数Service接口
 *
 * @author guet_developer01
 * @date 2026-05-11
 */
public interface IPhaseScintillationService {

    /**
     * 执行相位闪烁指数计算(定时任务调用)
     *
     * @return 计算结果列表
     */
    List<PhaseUtil.PhaseResultWithLevel> executeCalculation();

    /**
     * 保存相位闪烁指数计算结果到TDengine
     *
     * @param result 计算结果
     * @return 影响行数
     */
    int saveToTDengine(PhaseScintillation result);

    /**
     * 批量保存计算结果到TDengine
     *
     * @param results 计算结果列表
     * @return 影响行数数组
     */
    int[] batchSaveToTDengine(List<PhaseScintillation> results);

    /**
     * 查询最新的相位闪烁指数数据(用于大屏展示)
     *
     * @param limit 查询数量限制
     * @return 数据列表
     */
    List<PhaseScintillation> selectLatestData(int limit);

    /**
     * 根据监测站查询最新的相位闪烁指数数据
     *
     * @param stationId 监测站ID
     * @param limit 查询数量限制
     * @return 数据列表
     */
    List<PhaseScintillation> selectLatestByStation(String stationId, int limit);

    /**
     * 获取各监测站统计信息
     *
     * @return 统计信息
     */
    PhaseUtil.ScintillationSummary getScintillationSummary();
}