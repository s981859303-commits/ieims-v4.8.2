package com.ruoyi.ieims.satellitedataquery.mapper;

import com.ruoyi.ieims.ieimsgnssstation.domain.IeimsGnssStation;
import com.ruoyi.ieims.satellitedataquery.domain.StSatObs;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;
import java.util.Map;

/**
 * 卫星观测数据Mapper接口
 *
 * @author guet_developer01
 * @date 2026-04-26
 */
public interface StSatObsMapper
{
    /**
     * 查询卫星观测数据列表（从TDengine查询）
     *
     * @param params 查询参数Map
     * @return 卫星观测数据集合
     */
    public List<StSatObs> selectStSatObsList(Map<String, Object> params);

    /**
     * 保存查询记录到MySQL
     *
     * @param record 查询记录
     * @return 结果
     */
    public int insertQueryRecord(Map<String, Object> record);


    /**
     * 获取所有站点ID和名称列表（用于下拉选择）
     *
     * @return 站点信息列表
     */
    @Select("SELECT station_id, station_name FROM ieims_gnss_station WHERE del_flag = '0' ORDER BY station_id ASC")
    public List<IeimsGnssStation> selectAllStationIdAndName();
}