package com.ruoyi.ieims.gnss.domain;

import lombok.Data;
import java.io.Serializable;

/**
 * GNSS解算数据实体类
 *
 * @author guet_developer01
 * @date 2026-04-26
 */
@Data
public class GnssSolutionData implements Serializable {
    private static final long serialVersionUID = 1L;

    /** 站点ID */
    private String stationId;

    /** 数据产生时间戳（毫秒） */
    private Long time;

    /** 纬度 (十进制度数) */
    private Double latitude;

    /** 经度 (十进制度数) */
    private Double longitude;

    /** 海拔高程 (单位：米) */
    private Double altitude;

    /** 定位状态码 */
    private Integer status;

    /** 状态码对应的中文直观描述 */
    private String solutionType;

    /** 有效卫星数 */
    private Integer satelliteCount;

    /** 水平精度因子 */
    private Float hdop;

    /** 数据是否有效 */
    private Boolean valid;
}