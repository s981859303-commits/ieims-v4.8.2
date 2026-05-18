package com.ruoyi.ieims.gnss.mapper;

import java.util.List;
import com.ruoyi.ieims.gnss.domain.GnssStation;
import org.apache.ibatis.annotations.Param;

/**
 * GNSS监测站设备信息Mapper接口
 *
 * @author guet_developer01
 * @date 2026-04-26
 */
public interface GnssStationMapper
{
    /**
     * 查询GNSS监测站设备信息
     *
     * @param id GNSS监测站设备信息主键
     * @return GNSS监测站设备信息
     */
    public GnssStation selectGnssStationById(Long id);

    /**
     * 根据站点ID查询监测站信息
     *
     * @param stationId 站点ID
     * @return GNSS监测站设备信息
     */
    public GnssStation selectGnssStationByStationId(String stationId);

    /**
     * 查询GNSS监测站设备信息列表
     *
     * @param gnssStation GNSS监测站设备信息
     * @return GNSS监测站设备信息集合
     */
    public List<GnssStation> selectGnssStationList(GnssStation gnssStation);

    /**
     * 新增GNSS监测站设备信息
     *
     * @param gnssStation GNSS监测站设备信息
     * @return 结果
     */
    public int insertGnssStation(GnssStation gnssStation);

    /**
     * 修改GNSS监测站设备信息
     *
     * @param gnssStation GNSS监测站设备信息
     * @return 结果
     */
    public int updateGnssStation(GnssStation gnssStation);

    /**
     * 更新监测站在线状态
     *
     * @param stationId 站点ID
     * @param onlineStatus 在线状态
     * @param lastOnlineTime 最后在线时间
     * @return 结果
     */
    public int updateStationOnlineStatus(@Param("stationId") String stationId,
                                         @Param("onlineStatus") String onlineStatus,
                                         @Param("lastOnlineTime") java.util.Date lastOnlineTime);

    /**
     * 删除GNSS监测站设备信息
     *
     * @param id GNSS监测站设备信息主键
     * @return 结果
     */
    public int deleteGnssStationById(Long id);

    /**
     * 批量删除GNSS监测站设备信息
     *
     * @param ids 需要删除的数据主键集合
     * @return 结果
     */
    public int deleteGnssStationByIds(Long[] ids);
}