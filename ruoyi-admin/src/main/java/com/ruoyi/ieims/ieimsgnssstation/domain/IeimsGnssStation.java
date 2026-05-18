package com.ruoyi.ieims.ieimsgnssstation.domain;

import java.util.Date;
import com.fasterxml.jackson.annotation.JsonFormat;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import com.ruoyi.common.annotation.Excel;
import com.ruoyi.common.core.domain.BaseEntity;

/**
 * GNSS监测站设备信息对象 ieims_gnss_station
 * 
 * @author guet
 * @date 2026-04-27
 */
public class IeimsGnssStation extends BaseEntity
{
    private static final long serialVersionUID = 1L;

    /** 主键ID */
    private Long id;

    /** 站点ID（设备编号） */
    @Excel(name = "站点ID", readConverterExp = "设=备编号")
    private String stationId;

    /** 设备名称 */
    @Excel(name = "设备名称")
    private String stationName;

    /** 经度 */
    @Excel(name = "经度")
    private Double longitude;

    /** 纬度 */
    @Excel(name = "纬度")
    private Double latitude;

    /** 高度（米） */
    @Excel(name = "高度", readConverterExp = "米=")
    private Double altitude;

    /** 设备LOGO路径 */
    @Excel(name = "设备LOGO路径")
    private String logo;

    /** 所属部门ID */
    @Excel(name = "所属部门ID")
    private Long deptId;

    /** 负责人 */
    @Excel(name = "负责人")
    private String manager;

    /** 联系人 */
    @Excel(name = "联系人")
    private String contact;

    /** 联系电话 */
    @Excel(name = "联系电话")
    private String phone;

    /** 是否启用（0禁用 1启用） */
    @Excel(name = "是否启用", readConverterExp = "0=禁用,1=启用")
    private String enableFlag;

    /** 在线状态（0离线 1在线） */
    @Excel(name = "在线状态", readConverterExp = "0=离线,1=在线")
    private String onlineStatus;

    /** 最后在线时间 */
    @JsonFormat(pattern = "yyyy-MM-dd", timezone = "GMT+8")
    @Excel(name = "最后在线时间", width = 30, dateFormat = "yyyy-MM-dd")
    private Date lastOnlineTime;

    /** 删除标志(0正常 1删除) */
    private String delFlag;

    public void setId(Long id) 
    {
        this.id = id;
    }

    public Long getId() 
    {
        return id;
    }

    public void setStationId(String stationId) 
    {
        this.stationId = stationId;
    }

    public String getStationId() 
    {
        return stationId;
    }

    public void setStationName(String stationName) 
    {
        this.stationName = stationName;
    }

    public String getStationName() 
    {
        return stationName;
    }

    public void setLongitude(Double longitude) 
    {
        this.longitude = longitude;
    }

    public Double getLongitude() 
    {
        return longitude;
    }

    public void setLatitude(Double latitude) 
    {
        this.latitude = latitude;
    }

    public Double getLatitude() 
    {
        return latitude;
    }

    public void setAltitude(Double altitude) 
    {
        this.altitude = altitude;
    }

    public Double getAltitude() 
    {
        return altitude;
    }

    public void setLogo(String logo) 
    {
        this.logo = logo;
    }

    public String getLogo() 
    {
        return logo;
    }

    public void setDeptId(Long deptId) 
    {
        this.deptId = deptId;
    }

    public Long getDeptId() 
    {
        return deptId;
    }

    public void setManager(String manager) 
    {
        this.manager = manager;
    }

    public String getManager() 
    {
        return manager;
    }

    public void setContact(String contact) 
    {
        this.contact = contact;
    }

    public String getContact() 
    {
        return contact;
    }

    public void setPhone(String phone) 
    {
        this.phone = phone;
    }

    public String getPhone() 
    {
        return phone;
    }

    public void setEnableFlag(String enableFlag) 
    {
        this.enableFlag = enableFlag;
    }

    public String getEnableFlag() 
    {
        return enableFlag;
    }

    public void setOnlineStatus(String onlineStatus) 
    {
        this.onlineStatus = onlineStatus;
    }

    public String getOnlineStatus() 
    {
        return onlineStatus;
    }

    public void setLastOnlineTime(Date lastOnlineTime) 
    {
        this.lastOnlineTime = lastOnlineTime;
    }

    public Date getLastOnlineTime() 
    {
        return lastOnlineTime;
    }

    public void setDelFlag(String delFlag) 
    {
        this.delFlag = delFlag;
    }

    public String getDelFlag() 
    {
        return delFlag;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this,ToStringStyle.MULTI_LINE_STYLE)
            .append("id", getId())
            .append("stationId", getStationId())
            .append("stationName", getStationName())
            .append("longitude", getLongitude())
            .append("latitude", getLatitude())
            .append("altitude", getAltitude())
            .append("logo", getLogo())
            .append("deptId", getDeptId())
            .append("manager", getManager())
            .append("contact", getContact())
            .append("phone", getPhone())
            .append("enableFlag", getEnableFlag())
            .append("onlineStatus", getOnlineStatus())
            .append("lastOnlineTime", getLastOnlineTime())
            .append("remark", getRemark())
            .append("createBy", getCreateBy())
            .append("createTime", getCreateTime())
            .append("updateBy", getUpdateBy())
            .append("updateTime", getUpdateTime())
            .append("delFlag", getDelFlag())
            .toString();
    }
}
