package com.ruoyi.gnss.service;

/**
 * 卫星编号统一转换工具类
 *
 * 功能说明：
 * 1. 将不同数据源的卫星编号统一转换为标准格式
 * 2. GPS 卫星：G01、G02、G03 ... G32
 * 3. 北斗卫星：C01、C02、C03 ... C63
 *
 * 数据源映射：
 * - GPGSV：PRN号直接映射为 G01-G32
 * - GBGSV：PRN号直接映射为 C01-C63
 * - RTCM 1074：GPS MSM4，映射为 G01-G32
 * - RTCM 1127：BeiDou MSM4，映射为 C01-C63
 *
 * @author GNSS Team
 * @date 2026-03-25
 */
public class SatNoConverter {

    /** GPS 卫星系统标识 */
    public static final String SYS_GPS = "GPS";

    /** 北斗卫星系统标识 */
    public static final String SYS_BDS = "BDS";

    /** GLONASS 卫星系统标识 */
    public static final String SYS_GLO = "GLO";

    /** Galileo 卫星系统标识 */
    public static final String SYS_GAL = "GAL";

    /** GPS 卫星编号前缀 */
    public static final String PREFIX_GPS = "G";

    /** 北斗卫星编号前缀 */
    public static final String PREFIX_BDS = "C";

    /** GLONASS 卫星编号前缀 */
    public static final String PREFIX_GLO = "R";

    /** Galileo 卫星编号前缀 */
    public static final String PREFIX_GAL = "E";

    /** 未知系统前缀 */
    public static final String PREFIX_UNKNOWN = "X";

    /**
     * 将卫星系统和PRN号转换为统一格式
     *
     * @param satSystem 卫星系统（GPS/BDS/GLO/GAL）
     * @param prn PRN号
     * @return 统一格式的卫星编号（如 G01、C01）
     */
    public static String convert(String satSystem, int prn) {
        if (satSystem == null || prn <= 0) {
            return String.format("%s%02d", PREFIX_UNKNOWN, prn);
        }

        switch (satSystem.toUpperCase()) {
            case SYS_GPS:
                return String.format("%s%02d", PREFIX_GPS, prn);
            case SYS_BDS:
                return String.format("%s%02d", PREFIX_BDS, prn);
            case SYS_GLO:
                return String.format("%s%02d", PREFIX_GLO, prn);
            case SYS_GAL:
                return String.format("%s%02d", PREFIX_GAL, prn);
            default:
                return String.format("%s%02d", PREFIX_UNKNOWN, prn);
        }
    }

    /**
     * 从 RTCM 解算的卫星ID字符串解析并转换为统一格式
     *
     * RTCM 1074/1127 中的卫星ID格式可能为：
     * - 纯数字：如 "1"、"2"、"3"
     * - 带前缀：如 "G01"、"C01"
     *
     * @param satId 卫星ID字符串
     * @param rtcmMessageType RTCM消息类型（1074=GPS, 1127=BeiDou）
     * @return 统一格式的卫星编号
     */
    public static String fromRtcmSatId(String satId, int rtcmMessageType) {
        if (satId == null || satId.trim().isEmpty()) {
            return null;
        }

        satId = satId.trim();

        // 如果已经是标准格式（如 G01、C01），直接返回
        if (satId.matches("^[GCRE][0-9]{2}$")) {
            return satId;
        }

        // 如果是带前缀但格式不完全标准（如 G1、C1），补零
        if (satId.matches("^[GCRE][0-9]{1,2}$")) {
            char prefix = satId.charAt(0);
            int prn = Integer.parseInt(satId.substring(1));
            return String.format("%c%02d", prefix, prn);
        }

        // 纯数字格式，根据 RTCM 消息类型确定卫星系统
        try {
            int prn = Integer.parseInt(satId);
            return fromRtcmMessageType(rtcmMessageType, prn);
        } catch (NumberFormatException e) {
            // 解析失败，返回原始值
            return satId;
        }
    }

    /**
     * 根据 RTCM 消息类型和 PRN 号转换为统一格式
     *
     * @param rtcmMessageType RTCM消息类型
     * @param prn PRN号
     * @return 统一格式的卫星编号
     */
    public static String fromRtcmMessageType(int rtcmMessageType, int prn) {
        // RTCM 消息类型与卫星系统的对应关系
        // 1074 = GPS MSM4
        // 1127 = BeiDou MSM4
        // 1084 = GLONASS MSM4
        // 1094 = Galileo MSM4
        switch (rtcmMessageType) {
            case 1074:
                return String.format("%s%02d", PREFIX_GPS, prn);
            case 1127:
                return String.format("%s%02d", PREFIX_BDS, prn);
            case 1084:
                return String.format("%s%02d", PREFIX_GLO, prn);
            case 1094:
                return String.format("%s%02d", PREFIX_GAL, prn);
            default:
                return String.format("%s%02d", PREFIX_UNKNOWN, prn);
        }
    }

    /**
     * 从卫星编号解析卫星系统
     *
     * @param satNo 卫星编号（如 G01、C01）
     * @return 卫星系统（GPS/BDS/GLO/GAL/UNKNOWN）
     */
    public static String getSatSystem(String satNo) {
        if (satNo == null || satNo.isEmpty()) {
            return "UNKNOWN";
        }

        char prefix = satNo.charAt(0);
        switch (prefix) {
            case 'G':
                return SYS_GPS;
            case 'C':
                return SYS_BDS;
            case 'R':
                return SYS_GLO;
            case 'E':
                return SYS_GAL;
            default:
                return "UNKNOWN";
        }
    }

    /**
     * 从卫星编号解析 PRN 号
     *
     * @param satNo 卫星编号（如 G01、C01）
     * @return PRN号，解析失败返回 -1
     */
    public static int getPrn(String satNo) {
        if (satNo == null || satNo.length() < 2) {
            return -1;
        }

        try {
            return Integer.parseInt(satNo.substring(1));
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /**
     * 判断是否为 GPS 卫星
     */
    public static boolean isGps(String satNo) {
        return satNo != null && satNo.startsWith(PREFIX_GPS);
    }

    /**
     * 判断是否为北斗卫星
     */
    public static boolean isBds(String satNo) {
        return satNo != null && satNo.startsWith(PREFIX_BDS);
    }

    /**
     * 验证卫星编号格式是否正确
     *
     * @param satNo 卫星编号
     * @return true 表示格式正确
     */
    public static boolean isValid(String satNo) {
        if (satNo == null) {
            return false;
        }
        // 格式：一个字母 + 两位数字
        return satNo.matches("^[GCRE][0-9]{2}$");
    }
}
