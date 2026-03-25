package com.ruoyi.gnss.service;

import com.ruoyi.gnss.domain.SatObservation;
import java.util.List;

/**
 * 卫星观测数据存储服务接口
 *
 * 定义卫星观测数据的存储标准，支持多种数据库实现
 *
 * @author GNSS Team
 * @date 2026-03-25
 */
public interface ISatObservationStorageService {

    /**
     * 保存单条卫星观测数据
     *
     * @param observation 卫星观测数据
     * @return 是否保存成功
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
     * 按站点保存卫星观测数据
     *
     * @param stationId 站点ID
     * @param observations 卫星观测数据列表
     * @return 成功保存的数量
     */
    int saveSatObservationBatch(String stationId, List<SatObservation> observations);

    /**
     * 检查服务是否已初始化
     *
     * @return true 表示已初始化
     */
    boolean isInitialized();
}
