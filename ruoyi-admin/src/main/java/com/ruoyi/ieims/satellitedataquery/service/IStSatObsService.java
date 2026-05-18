package com.ruoyi.ieims.satellitedataquery.service;

import com.ruoyi.common.core.domain.AjaxResult;
import com.ruoyi.ieims.satellitedataquery.domain.StSatObs;

import java.util.List;

/**
 * 卫星观测数据Service接口
 *
 * @author guet_developer01
 * @date 2026-04-26
 */
public interface IStSatObsService
{
    /**
     * 查询卫星观测数据列表（从TDengine）
     *
     * @param stSatObs 查询条件
     * @return 卫星观测数据集合
     */
    public List<StSatObs> selectStSatObsList(StSatObs stSatObs);

    /**
     * 导出卫星观测数据
     *
     * @param stSatObs 查询条件
     * @return 导出文件路径
     */
    public String exportStSatObs(StSatObs stSatObs);
}