package com.ruoyi.ieims.gnss.service;

import com.ruoyi.ieims.gnss.domain.TecResult;

import java.util.List;
import java.util.Map;

/**
 * TEC结果数据 Service接口
 */
public interface ITecResultService {

    /* ========== 原有业务方法保留，下方为新增 ========== */

    /**
     * 查询指定站点最近N条数据
     */
    List<TecResult> selectLatestByStation(String stationId, int limit);

    /**
     * 查询大屏摘要统计（最近1小时各站点汇总）
     */
    Map<String, Object> selectLatestSummary();
}