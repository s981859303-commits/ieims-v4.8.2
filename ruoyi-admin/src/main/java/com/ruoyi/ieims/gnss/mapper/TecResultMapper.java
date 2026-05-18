package com.ruoyi.ieims.gnss.mapper;

import com.ruoyi.ieims.gnss.domain.TecResult;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;


import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 * TEC结果数据Mapper
 *
 * @author guet_developer01
 * @date 2026-05-12
 */
@Mapper
public interface TecResultMapper {

    /**
     * 查询TEC结果列表
     */
    List<TecResult> selectTecResultList(@Param("stationId") String stationId,
                                        @Param("satNo") String satNo,
                                        @Param("beginTime") Date beginTime,
                                        @Param("endTime") Date endTime);

    /**
     * 查询指定站点最近N条TEC数据（按时间倒序）
     */
    List<TecResult> selectLatestByStation(@Param("stationId") String stationId,
                                          @Param("limit") int limit);

    /**
     * 查询最近1小时各站点汇总统计（大屏顶部卡片用）
     */
    Map<String, Object> selectLatestSummary();
}