package com.ruoyi.ieims.satellitedataquery.domain;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.ruoyi.common.annotation.Excel;
import com.ruoyi.common.core.domain.BaseEntity;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Size;
import java.util.Date;

/**
 * 卫星观测数据实体类
 *
 * @author guet_developer01
 * @date 2026-04-26
 */
public class StSatObs extends BaseEntity
{
    private static final long serialVersionUID = 1L;

    /** 主键ID（MySQL记录表ID） */
    private Long id;

    /** 时间戳 */
    @Excel(name = "时间戳", width = 30, dateFormat = "yyyy-MM-dd HH:mm:ss")
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss.SSS")
    private Date ts;

    /** 观测唯一键 */
    @Excel(name = "观测唯一键")
    private String obsUniqueKey;

    /** 数据来源 (GSV=仅星历视图) */
    @Excel(name = "数据来源")
    private String dataSource;

    /** 是否完整 */
    @Excel(name = "是否完整")
    private Boolean isComplete;

    /** 本地时间戳 */
    private Long localTimestamp;

    /** 日期来源 (ZDA=报文解析 / SYSTEM=系统时间) */
    @Excel(name = "日期来源")
    private String dateSource;

    /** 是否来自ZDA */
    private Boolean dateFromZda;

    /** 观测时间 */
    @Excel(name = "观测时间")
    private String observationTime;

    /** 高度角 */
    @Excel(name = "高度角")
    private Double elevation;

    /** 方位角 */
    @Excel(name = "方位角")
    private Double azimuth;

    /** 信噪比 */
    @Excel(name = "信噪比")
    private Double snr;

    /** 伪距P1 */
    @Excel(name = "伪距P1")
    private Double pseudorangeP1;

    /** 伪距P2 */
    @Excel(name = "伪距P2")
    private Double pseudorangeP2;

    /** 相位L1 */
    @Excel(name = "相位L1")
    private Double phaseL1;

    /** 相位P2 */
    @Excel(name = "相位P2")
    private Double phaseP2;

    /** C1码 */
    @Excel(name = "C1码")
    private String c1;

    /** C2码 */
    @Excel(name = "C2码")
    private String c2;

    /** 站点ID（TAG） */
    @Excel(name = "站点ID")
    @NotBlank(message = "站点ID不能为空")
    @Size(max = 64, message = "站点ID长度不能超过64个字符")
    private String stationId;

    /** 卫星ID（TAG） */
    @Excel(name = "卫星ID")
    private String satNo;

    /** 卫星系统（TAG） */
    @Excel(name = "卫星系统")
    private String satSystem;

    /** 查询参数-起始时间 */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date queryStartTime;

    /** 查询参数-结束时间 */
    @JsonFormat(pattern = "yyyy-MM-dd HH:mm:ss")
    private Date queryEndTime;

    /** 查询参数-数据限量 */
    private Integer queryLimit = 100;

    /** 查询参数-是否实时 */
    private Boolean isRealtime;

    public Long getId()
    {
        return id;
    }

    public void setId(Long id)
    {
        this.id = id;
    }

    public Date getTs()
    {
        return ts;
    }

    public void setTs(Date ts)
    {
        this.ts = ts;
    }

    public String getObsUniqueKey()
    {
        return obsUniqueKey;
    }

    public void setObsUniqueKey(String obsUniqueKey)
    {
        this.obsUniqueKey = obsUniqueKey;
    }

    public String getDataSource()
    {
        return dataSource;
    }

    public void setDataSource(String dataSource)
    {
        this.dataSource = dataSource;
    }

    public Boolean getIsComplete()
    {
        return isComplete;
    }

    public void setIsComplete(Boolean isComplete)
    {
        this.isComplete = isComplete;
    }

    public Long getLocalTimestamp()
    {
        return localTimestamp;
    }

    public void setLocalTimestamp(Long localTimestamp)
    {
        this.localTimestamp = localTimestamp;
    }

    public String getDateSource()
    {
        return dateSource;
    }

    public void setDateSource(String dateSource)
    {
        this.dateSource = dateSource;
    }

    public Boolean getDateFromZda()
    {
        return dateFromZda;
    }

    public void setDateFromZda(Boolean dateFromZda)
    {
        this.dateFromZda = dateFromZda;
    }

    public String getObservationTime()
    {
        return observationTime;
    }

    public void setObservationTime(String observationTime)
    {
        this.observationTime = observationTime;
    }

    public Double getElevation()
    {
        return elevation;
    }

    public void setElevation(Double elevation)
    {
        this.elevation = elevation;
    }

    public Double getAzimuth()
    {
        return azimuth;
    }

    public void setAzimuth(Double azimuth)
    {
        this.azimuth = azimuth;
    }

    public Double getSnr()
    {
        return snr;
    }

    public void setSnr(Double snr)
    {
        this.snr = snr;
    }

    public Double getPseudorangeP1()
    {
        return pseudorangeP1;
    }

    public void setPseudorangeP1(Double pseudorangeP1)
    {
        this.pseudorangeP1 = pseudorangeP1;
    }

    public Double getPseudorangeP2()
    {
        return pseudorangeP2;
    }

    public void setPseudorangeP2(Double pseudorangeP2)
    {
        this.pseudorangeP2 = pseudorangeP2;
    }

    public Double getPhaseL1()
    {
        return phaseL1;
    }

    public void setPhaseL1(Double phaseL1)
    {
        this.phaseL1 = phaseL1;
    }

    public Double getPhaseP2()
    {
        return phaseP2;
    }

    public void setPhaseP2(Double phaseP2)
    {
        this.phaseP2 = phaseP2;
    }

    public String getC1()
    {
        return c1;
    }

    public void setC1(String c1)
    {
        this.c1 = c1;
    }

    public String getC2()
    {
        return c2;
    }

    public void setC2(String c2)
    {
        this.c2 = c2;
    }

    public String getStationId()
    {
        return stationId;
    }

    public void setStationId(String stationId)
    {
        this.stationId = stationId;
    }

    public String getSatNo()
    {
        return satNo;
    }

    public void setSatNo(String satNo)
    {
        this.satNo = satNo;
    }

    public String getSatSystem()
    {
        return satSystem;
    }

    public void setSatSystem(String satSystem)
    {
        this.satSystem = satSystem;
    }

    public Date getQueryStartTime()
    {
        return queryStartTime;
    }

    public void setQueryStartTime(Date queryStartTime)
    {
        this.queryStartTime = queryStartTime;
    }

    public Date getQueryEndTime()
    {
        return queryEndTime;
    }

    public void setQueryEndTime(Date queryEndTime)
    {
        this.queryEndTime = queryEndTime;
    }

    public Integer getQueryLimit()
    {
        return queryLimit;
    }

    public void setQueryLimit(Integer queryLimit)
    {
        this.queryLimit = queryLimit;
    }

    public Boolean getIsRealtime()
    {
        return isRealtime;
    }

    public void setIsRealtime(Boolean isRealtime)
    {
        this.isRealtime = isRealtime;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.MULTI_LINE_STYLE)
                .append("id", getId())
                .append("ts", getTs())
                .append("obsUniqueKey", getObsUniqueKey())
                .append("dataSource", getDataSource())
                .append("isComplete", getIsComplete())
                .append("localTimestamp", getLocalTimestamp())
                .append("dateSource", getDateSource())
                .append("dateFromZda", getDateFromZda())
                .append("observationTime", getObservationTime())
                .append("elevation", getElevation())
                .append("azimuth", getAzimuth())
                .append("snr", getSnr())
                .append("pseudorangeP1", getPseudorangeP1())
                .append("pseudorangeP2", getPseudorangeP2())
                .append("phaseL1", getPhaseL1())
                .append("phaseP2", getPhaseP2())
                .append("c1", getC1())
                .append("c2", getC2())
                .append("stationId", getStationId())
                .append("satNo", getSatNo())
                .append("satSystem", getSatSystem())
                .append("createBy", getCreateBy())
                .append("createTime", getCreateTime())
                .append("updateBy", getUpdateBy())
                .append("updateTime", getUpdateTime())
                .append("remark", getRemark())
                .toString();
    }
}