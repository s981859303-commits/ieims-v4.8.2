package com.ruoyi.ieims.util;


/**
 * TEC计算常量定义
 *
 * @author guet_developer01
 * @date 2026-05-12
 */
public final class TecConstant {

    private TecConstant() {
    }

    public static final double SPEED_OF_LIGHT = 2.99792458e8;
    public static final double TEC_UNIT = 1e16;
    public static final double COEFFICIENT = 40.308;
    public static final double IONOSPHERE_HEIGHT = 350000.0;
    public static final double EARTH_RADIUS = 6371000.0;

    /** 最低仰角截止（度） */
    public static final double MIN_ELEVATION = 15.0;
    /** 最大仰角跨度（度） */
    public static final double MAX_ELEV_SPAN = 30.0;
    /** 最小仰角跨度（度） */
    public static final double MIN_ELEV_SPAN = 5.0;
    /** 伪距粗差阈值（TECU） */
    public static final double PSEUDORANGE_JUMP_THRESHOLD = 50.0;
    /** 最小有效历元数 */
    public static final int MIN_VALID_EPOCHS = 15;
    /** 数据中断阈值（秒） */
    public static final double MAX_GAP_SECONDS = 60.0;
    /** 弧段TTL（分钟） */
    public static final long ARC_TTL_MINUTES = 60;
    /** 滑动窗口最大长度（分钟） */
    public static final int MAX_ARC_MINUTES = 30;

    /** MW周跳阈值（周） */
    public static final double MW_SLIP_THRESHOLD_CYCLES = 3.0;
    /** GF周跳阈值（米） */
    public static final double GF_SLIP_THRESHOLD_METERS = 0.05;

    public static final String QUALITY_GOOD = "GOOD";
    public static final String QUALITY_SUSPECT = "SUSPECT";
    public static final String QUALITY_RELATIVE = "RELATIVE";
    public static final String QUALITY_POOR = "POOR";

    public static final String MAPPING_SLM = "SLM";
}