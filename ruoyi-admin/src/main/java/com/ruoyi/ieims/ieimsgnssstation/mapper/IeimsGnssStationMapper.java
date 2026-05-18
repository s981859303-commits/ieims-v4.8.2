package com.ruoyi.ieims.ieimsgnssstation.mapper;

import java.util.List;
import com.ruoyi.ieims.ieimsgnssstation.domain.IeimsGnssStation;
import org.apache.ibatis.annotations.Select;

/**
 * GNSS监测站设备信息Mapper接口
 * 
 * @author guet
 * @date 2026-04-27
 */
public interface IeimsGnssStationMapper 
{
    /**
     * 查询GNSS监测站设备信息
     * 
     * @param id GNSS监测站设备信息主键
     * @return GNSS监测站设备信息
     */
    public IeimsGnssStation selectIeimsGnssStationById(Long id);

    /**
     * 查询GNSS监测站设备信息列表
     * 
     * @param ieimsGnssStation GNSS监测站设备信息
     * @return GNSS监测站设备信息集合
     */
    public List<IeimsGnssStation> selectIeimsGnssStationList(IeimsGnssStation ieimsGnssStation);

    /**
     * 新增GNSS监测站设备信息
     * 
     * @param ieimsGnssStation GNSS监测站设备信息
     * @return 结果
     */
    public int insertIeimsGnssStation(IeimsGnssStation ieimsGnssStation);

    /**
     * 修改GNSS监测站设备信息
     * 
     * @param ieimsGnssStation GNSS监测站设备信息
     * @return 结果
     */
    public int updateIeimsGnssStation(IeimsGnssStation ieimsGnssStation);

    /**
     * 删除GNSS监测站设备信息
     * 
     * @param id GNSS监测站设备信息主键
     * @return 结果
     */
    public int deleteIeimsGnssStationById(Long id);

    /**
     * 批量删除GNSS监测站设备信息
     * 
     * @param ids 需要删除的数据主键集合
     * @return 结果
     */
    public int deleteIeimsGnssStationByIds(String[] ids);
}
