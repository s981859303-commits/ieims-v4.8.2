package com.ruoyi.ieims.satellite.domain;

import lombok.Data;
import java.io.Serializable;

/**
 * 卫星观测数据实体类
 *
 * @author guet_developer01
 * @date 2026-04-30
 */
@Data
public class SatelliteObsData implements Serializable {
    private static final long serialVersionUID = 1L;

    private String stationId;
    private String obsUniqueKey;
    private String satNo;
    private String satSystem;
    private String dataSource;
    private Boolean complete;

    private Long epochTime;
    private Long timestamp;
    private String dateSource;
    private Boolean dateFromZda;
    private String observationTime;

    private Double elevation;
    private Double azimuth;
    private Double snr;

    private Double pseudorangeP1;
    private Double pseudorangeP2;
    private Double phaseL1;
    private Double phaseP2;
    private String c1;
    private String c2;
}