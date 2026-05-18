package com.ruoyi.ieims.gnss.service;

import com.ruoyi.ieims.gnss.domain.TecResult;

import java.util.Date;
import java.util.List;

/**
 * TEC计算服务接口
 *
 * @author guet_developer01
 * @date 2026-05-12
 */
public interface ITecCalculationService {

    /**
     * 查询TEC结果列表
     */
    List<TecResult> selectTecResultList(String stationId, String satNo, Date beginTime, Date endTime);

    /**
     * 执行TEC计算并入库
     */
    void computeAndStoreTec();

    /**
     * 获取某站点最新TEC数据
     */
    List<TecResult> getLatestTecByStation(String stationId, int limit);
}