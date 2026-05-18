package com.ruoyi.ieims.gnss.mapper;

import org.apache.ibatis.annotations.Param;

import java.util.List;
import java.util.Map;

/**
 * 相位闪烁指数Mapper接口
 *
 * @author guet_developer01
 * @date 2026-05-11
 */
public interface PhaseScintillationMapper {

    /**
     * 获取需要计算的监测站和卫星列表
     * 查询最近1分钟内有数据的station_id和sat_no去重组合
     *
     * @param startTime 开始时间(毫秒)
     * @param endTime 结束时间(毫秒)
     * @return 去重后的station_id,sat_no列表
     */
    List<Map<String, String>> getDistinctStationSatellite(@Param("startTime") Long startTime,
                                                          @Param("endTime") Long endTime);

    /**
     * 获取指定监测站和卫星最近1分钟内的相位数据
     *
     * @param stationId 监测站ID
     * @param satNo 卫星编号
     * @param startTime 开始时间(毫秒)
     * @param endTime 结束时间(毫秒)
     * @return 相位数据列表 {ts, phase_l1, local_timestamp}
     */
    List<Map<String, Object>> getPhaseData(@Param("stationId") String stationId,
                                           @Param("satNo") String satNo,
                                           @Param("startTime") Long startTime,
                                           @Param("endTime") Long endTime);
}