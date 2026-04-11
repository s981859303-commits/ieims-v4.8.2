package com.ruoyi.gnss.service;

/**
 * RTCM 数据存储服务接口
 *
 * 功能说明：
 * 1. 定义 RTCM 原始数据存储的标准接口
 * 2. 支持多种存储后端实现（TDengine、InfluxDB 等）
 * 3. 遵循依赖倒置原则
 *
 * @author GNSS Team
 * @date 2026-03-25
 */
public interface IRtcmStorageService {

    /**
     * 检查服务是否已初始化
     *
     * @return true 表示已初始化
     */
    boolean isInitialized();

    /**
     * 保存 RTCM 原始数据（指定站点）
     *
     * @param stationId 站点ID
     * @param rtcmData RTCM 二进制数据
     * @return true 表示保存成功
     */
    boolean saveRtcmRawData(String stationId, byte[] rtcmData);

    /**
     * 获取存储统计信息
     *
     * @return 统计信息字符串
     */
    default String getStatistics() {
        return "Not implemented";
    }
}
