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
    public static final char PREFIX_GPS = 'G';

    /** 北斗卫星编号前缀 */
    public static final char PREFIX_BDS = 'C';

    /** GLONASS 卫星编号前缀 */
    public static final char PREFIX_GLO = 'R';

    /** Galileo 卫星编号前缀 */
    public static final char PREFIX_GAL = 'E';

    /** 未知系统前缀 */
    public static final char PREFIX_UNKNOWN = 'X';

    /**
     * 将卫星系统和PRN号转换为统一格式
     *
     * @param satSystem 卫星系统（GPS/BDS/GLO/GAL）
     * @param prn PRN号
     * @return 统一格式的卫星编号（如 G01、C01）
     */
    public static String convert(String satSystem, int prn) {
        if (satSystem == null || prn <= 0) {
            return formatSatNo(PREFIX_UNKNOWN, prn);
        }

        char prefix;
        switch (satSystem.toUpperCase()) {
            case SYS_GPS:
                prefix = PREFIX_GPS;
                break;
            case SYS_BDS:
                prefix = PREFIX_BDS;
                break;
            case SYS_GLO:
                prefix = PREFIX_GLO;
                break;
            case SYS_GAL:
                prefix = PREFIX_GAL;
                break;
            default:
                prefix = PREFIX_UNKNOWN;
                break;
        }
        return formatSatNo(prefix, prn);
    }

    /**
     * 从 RTCM 解算的卫星ID字符串解析并转换为统一格式
     *
     * 优化：移除正则表达式，改用字符判断
     *
     * @param satId 卫星ID字符串
     * @param rtcmMessageType RTCM消息类型（1074=GPS, 1127=BeiDou）
     * @return 统一格式的卫星编号
     */
    public static String fromRtcmSatId(String satId, int rtcmMessageType) {
        if (satId == null || satId.isEmpty()) {
            return null;
        }

        satId = satId.trim();
        int len = satId.length();

        // 检查是否已经是标准格式（如 G01、C01）
        if (len == 3 && isSatPrefix(satId.charAt(0)) && isDigit(satId.charAt(1)) && isDigit(satId.charAt(2))) {
            return satId;
        }

        // 检查是否是带前缀但格式不完全标准（如 G1、C1），补零
        if (len == 2 && isSatPrefix(satId.charAt(0)) && isDigit(satId.charAt(1))) {
            return formatSatNo(satId.charAt(0), satId.charAt(1) - '0');
        }

        // 纯数字格式，根据 RTCM 消息类型确定卫星系统
        try {
            int prn = Integer.parseInt(satId);
            return fromRtcmMessageType(rtcmMessageType, prn);
        } catch (NumberFormatException e) {
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
        char prefix;
        switch (rtcmMessageType) {
            case 1074:
                prefix = PREFIX_GPS;
                break;
            case 1127:
                prefix = PREFIX_BDS;
                break;
            case 1084:
                prefix = PREFIX_GLO;
                break;
            case 1094:
                prefix = PREFIX_GAL;
                break;
            default:
                prefix = PREFIX_UNKNOWN;
                break;
        }
        return formatSatNo(prefix, prn);
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

        switch (satNo.charAt(0)) {
            case PREFIX_GPS:
                return SYS_GPS;
            case PREFIX_BDS:
                return SYS_BDS;
            case PREFIX_GLO:
                return SYS_GLO;
            case PREFIX_GAL:
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
        return satNo != null && !satNo.isEmpty() && satNo.charAt(0) == PREFIX_GPS;
    }

    /**
     * 判断是否为北斗卫星
     */
    public static boolean isBds(String satNo) {
        return satNo != null && !satNo.isEmpty() && satNo.charAt(0) == PREFIX_BDS;
    }

    /**
     * 验证卫星编号格式是否正确
     *
     * 优化：移除正则表达式，改用字符判断
     *
     * @param satNo 卫星编号
     * @return true 表示格式正确
     */
    public static boolean isValid(String satNo) {
        if (satNo == null || satNo.length() != 3) {
            return false;
        }
        return isSatPrefix(satNo.charAt(0))
                && isDigit(satNo.charAt(1))
                && isDigit(satNo.charAt(2));
    }

    // ==================== 私有辅助方法 ====================

    /**
     * 格式化卫星编号
     */
    private static String formatSatNo(char prefix, int prn) {
        return String.format("%c%02d", prefix, prn);
    }

    /**
     * 判断字符是否为卫星前缀
     */
    private static boolean isSatPrefix(char c) {
        return c == PREFIX_GPS || c == PREFIX_BDS || c == PREFIX_GLO || c == PREFIX_GAL;
    }

    /**
     * 判断字符是否为数字
     */
    private static boolean isDigit(char c) {
        return c >= '0' && c <= '9';
    }
}
