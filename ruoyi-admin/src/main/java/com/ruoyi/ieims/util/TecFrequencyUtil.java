package com.ruoyi.ieims.util;

/**
 * GNSS频率与波长工具
 *
 * @author guet_developer01
 * @date 2026-05-12
 */
public final class TecFrequencyUtil {

    private TecFrequencyUtil() {}

    public static double getFrequency(String satNo, int band) {
        if (satNo == null || satNo.length() < 1) {
            throw new IllegalArgumentException("卫星编号不能为空");
        }
        char sys = satNo.toUpperCase().charAt(0);
        int prn = -1;
        try {
            prn = Integer.parseInt(satNo.substring(1));
        } catch (NumberFormatException e) {
            prn = -1;
        }

        if (sys == 'G') {
            // GPS L1/L2 P(Y)
            return band == 1 ? 1575.42e6 : 1227.60e6;
        } else if (sys == 'C') {
            // 北斗 B1I + B2I（所有 C 系列卫星统一处理，PRN 区分可后续细化）
            // B1I: 1561.098 MHz, B2I/B3I: 1207.140 MHz
            return band == 1 ? 1561.098e6 : 1207.140e6;
        } else if (sys == 'E') {
            // Galileo E1 + E5a（RTCM 1127 MSM 常用）
            return band == 1 ? 1575.42e6 : 1176.45e6;
        } else if (sys == 'R') {
            // GLONASS G1/G2（若后续支持）
            return band == 1 ? 1602.00e6 : 1246.00e6;
        }
        throw new IllegalArgumentException("暂不支持的卫星系统标识: " + sys + " (satNo=" + satNo + ")");
    }

    public static double getWavelength(String satNo, int band) {
        return TecConstant.SPEED_OF_LIGHT / getFrequency(satNo, band);
    }
}