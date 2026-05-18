package com.ruoyi.ieims.gnss.domain;

import com.ruoyi.common.core.domain.BaseEntity;
import lombok.Data;
import lombok.EqualsAndHashCode;

import java.util.Date;

/**
 * TEC计算结果实体（映射TDengine超级表）
 *
 * @author guet_developer01
 * @date 2026-05-12
 */
@Data
@EqualsAndHashCode(callSuper = true)
public class TecResult extends BaseEntity {

    private static final long serialVersionUID = 1L;

    /** 主键ID（MySQL展示用，TDengine无此列） */
    private Long id;

    /** 站点ID */
    private String stationId;

    /** 卫星编号 */
    private String satNo;

    /** 时间戳 */
    private Date ts;

    /** 斜路径TEC */
    private Double stec;

    /** 垂直TEC */
    private Double vtec;

    /** 电离层穿刺点纬度（°） */
    private Double ippLat;

    /** 电离层穿刺点经度（°） */
    private Double ippLon;

    /** 相位-伪距偏差估计（含模糊度与残余DCB，单位TECU） */
    private Double dcbEstimate;

    /** 有效历元数 */
    private Integer validEpochCount;

    /** 仰角跨度 */
    private Double elevSpan;

    /** 质量标识：GOOD/SUSPECT/RELATIVE/POOR */
    private String qualityFlag;

    /** 是否已进行DCB校正 */
    private Boolean dcbCorrected;

    /** 周跳次数 */
    private Integer slipCount;

    /** 映射函数类型 */
    private String mappingFunc;

    /** 卫星系统 G/C/E */
    private String satSystem;
}