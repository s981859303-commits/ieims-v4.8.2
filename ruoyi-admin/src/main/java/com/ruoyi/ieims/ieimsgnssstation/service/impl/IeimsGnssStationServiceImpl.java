package com.ruoyi.ieims.ieimsgnssstation.service.impl;

import java.util.List;
import com.ruoyi.common.utils.DateUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.ruoyi.ieims.ieimsgnssstation.mapper.IeimsGnssStationMapper;
import com.ruoyi.ieims.ieimsgnssstation.domain.IeimsGnssStation;
import com.ruoyi.ieims.ieimsgnssstation.service.IIeimsGnssStationService;
import com.ruoyi.common.core.text.Convert;

/**
 * GNSS监测站设备信息Service业务层处理
 * 
 * @author guet
 * @date 2026-04-27
 */
@Service
public class IeimsGnssStationServiceImpl implements IIeimsGnssStationService 
{
    @Autowired
    private IeimsGnssStationMapper ieimsGnssStationMapper;

    /**
     * 查询GNSS监测站设备信息
     * 
     * @param id GNSS监测站设备信息主键
     * @return GNSS监测站设备信息
     */
    @Override
    public IeimsGnssStation selectIeimsGnssStationById(Long id)
    {
        return ieimsGnssStationMapper.selectIeimsGnssStationById(id);
    }

    /**
     * 查询GNSS监测站设备信息列表
     * 
     * @param ieimsGnssStation GNSS监测站设备信息
     * @return GNSS监测站设备信息
     */
    @Override
    public List<IeimsGnssStation> selectIeimsGnssStationList(IeimsGnssStation ieimsGnssStation)
    {
        return ieimsGnssStationMapper.selectIeimsGnssStationList(ieimsGnssStation);
    }

    /**
     * 新增GNSS监测站设备信息
     * 
     * @param ieimsGnssStation GNSS监测站设备信息
     * @return 结果
     */
    @Override
    public int insertIeimsGnssStation(IeimsGnssStation ieimsGnssStation)
    {
        ieimsGnssStation.setCreateTime(DateUtils.getNowDate());
        return ieimsGnssStationMapper.insertIeimsGnssStation(ieimsGnssStation);
    }

    /**
     * 修改GNSS监测站设备信息
     * 
     * @param ieimsGnssStation GNSS监测站设备信息
     * @return 结果
     */
    @Override
    public int updateIeimsGnssStation(IeimsGnssStation ieimsGnssStation)
    {
        ieimsGnssStation.setUpdateTime(DateUtils.getNowDate());
        return ieimsGnssStationMapper.updateIeimsGnssStation(ieimsGnssStation);
    }

    /**
     * 批量删除GNSS监测站设备信息
     * 
     * @param ids 需要删除的GNSS监测站设备信息主键
     * @return 结果
     */
    @Override
    public int deleteIeimsGnssStationByIds(String ids)
    {
        return ieimsGnssStationMapper.deleteIeimsGnssStationByIds(Convert.toStrArray(ids));
    }

    /**
     * 删除GNSS监测站设备信息信息
     * 
     * @param id GNSS监测站设备信息主键
     * @return 结果
     */
    @Override
    public int deleteIeimsGnssStationById(Long id)
    {
        return ieimsGnssStationMapper.deleteIeimsGnssStationById(id);
    }
}
