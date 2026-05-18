package com.ruoyi.ieims.gnss.domain;

import lombok.Data;
import java.util.Date;

/**
 * TEC计算专用观测数据对象
 *
 * @author guet_developer01
 * @date 2026-05-12
 */
@Data
public class TecSatObs {

    /** 时间戳 */
    private Date ts;

    /** 站点ID（TAG） */
    private String stationId;

    /** 卫星编号（TAG） */
    private String satNo;

    /** 卫星系统（TAG） */
    private String satSystem;

    /** 仰角（度） */
    private Double elevation;

    /** 方位角（度） */
    private Double azimuth;

    /** 伪距P1（米） */
    private Double pseudorangeP1;

    /** 伪距P2（米） */
    private Double pseudorangeP2;

    /** 载波相位L1（周） */
    private Double phaseL1;

    /** 载波相位L2（周）--- 对应表字段 phase_p2 */
    private Double phaseP2;
}