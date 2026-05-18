package com.ruoyi.ieims.satellite.service;

import com.ruoyi.ieims.satellite.domain.SatelliteObsData;
import java.util.List;

/**
 * 卫星观测数据 服务层接口
 *
 * @author guet_developer01
 * @date 2026-04-30
 */
public interface ISatelliteObsService {
    /**
     * 批量保存卫星观测数据到TDengine
     * @param dataList 卫星数据集合
     */
    void batchSaveToTDengine(List<SatelliteObsData> dataList);
}