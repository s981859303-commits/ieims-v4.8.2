package com.ruoyi.ieims.gnss.service;

import com.ruoyi.ieims.gnss.domain.GnssSolution;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Date;

/**
 * GGA 语句解析器
 *
 * 功能说明：
 * 1. 解析 $GNGGA / $GPGGA / $BDGGA / $GLGGA / $GAGGA 语句
 * 2. 提取经纬度、高程、定位状态（单点/差分/固定/浮点）、参与解算卫星数、HDOP
 * 3. 包含 NMEA 校验和 (Checksum) 验证
 * 4. 安全处理空字段和格式异常
 *
 * GGA 语句格式：
 * $xxGGA,time,lat,N/S,lon,E/W,fix,sats,hdop,alt,M,geo,M,age,stn*CC
 *
 * @version 1.0 - 从 MixedLogSplitter 中重构抽离
 */
@Component
public class GgaParser {

    private static final Logger logger = LoggerFactory.getLogger(GgaParser.class);

    // ==================== 公共接口 ====================

    /**
     * 判断是否为 GGA 语句
     *
     * @param nmea NMEA 语句
     * @return true 表示是 GGA 语句
     */
    public boolean isGgaSentence(String nmea) {
        if (nmea == null || nmea.isEmpty()) {
            return false;
        }
        String trimmed = nmea.trim();
        return trimmed.startsWith("$GPGGA") ||
                trimmed.startsWith("$GNGGA") ||
                trimmed.startsWith("$BDGGA") ||
                trimmed.startsWith("$GLGGA") ||
                trimmed.startsWith("$GAGGA");
    }

    /**
     * 解析 GGA 语句
     *
     * @param nmea NMEA 语句字符串
     * @return GnssSolution 定位解算实体，解析失败或无效则返回 null
     */
    public GnssSolution parse(String nmea) {
        if (!isGgaSentence(nmea)) {
            return null;
        }

        // 校验 Checksum 保证数据未在传输中损坏
        if (!validateChecksum(nmea)) {
            logger.debug("GGA checksum校验失败: {}", truncateForLog(nmea));
            return null;
        }

        try {
            // 移除 checksum 部分后按逗号分割
            int starIndex = nmea.indexOf('*');
            String contentPart = starIndex > 0 ? nmea.substring(0, starIndex) : nmea;
            String[] fields = contentPart.split(",", -1); // 传入 -1 保留末尾的空字符串

            // GGA 至少需要 10 个字段才能提取出坐标和高程
            if (fields.length < 10) {
                logger.warn("GGA字段数不足，无法解析: {}", truncateForLog(nmea));
                return null;
            }

            // 提取经纬度
            double lat = parseNmeaCoord(fields[2], fields[3]);
            double lon = parseNmeaCoord(fields[4], fields[5]);

            // 提取定位状态 (0=无效, 1=单点, 2=差分, 4=固定解, 5=浮点解)
            int status = parsePositiveInt(fields[6]);

            // 提取卫星数
            int sats = parsePositiveInt(fields[7]);

            // 提取水平精度因子 HDOP
            double hdop = parseDouble(fields[8]);

            // 提取海拔高程
            double alt = parseDouble(fields[9]);

            // 封装结果对象 (直接使用系统当前时间作为接收时间，也可后续加入 ZDA 融合逻辑)
            return new GnssSolution(new Date(), lat, lon, alt, status, sats).setHdop(hdop);

        } catch (Exception e) {
            logger.warn("GGA解析异常: {} - {}", truncateForLog(nmea), e.getMessage());
            return null;
        }
    }

    // ==================== 私有辅助方法 ====================

    /**
     * 解析 NMEA 格式的经纬度 (如：ddmm.mmmm)
     * 转换为十进制度数 (Decimal Degrees)
     *
     * @param value 坐标数值字符串
     * @param dir   方向 (N/S/E/W)
     * @return 十进制度数坐标
     */
    private double parseNmeaCoord(String value, String dir) {
        if (value == null || value.trim().isEmpty()) {
            return 0.0;
        }
        try {
            int dot = value.indexOf('.');
            if (dot < 2) return 0.0; // 格式异常防御

            int degLen = dot - 2;
            double deg = Double.parseDouble(value.substring(0, degLen));
            double min = Double.parseDouble(value.substring(degLen));

            // 转换公式：度 + 分/60
            double coord = deg + min / 60.0;

            // 南半球和西半球为负数
            if ("S".equalsIgnoreCase(dir) || "W".equalsIgnoreCase(dir)) {
                coord = -coord;
            }
            return coord;
        } catch (Exception e) {
            logger.debug("NMEA坐标解析失败: value={}, dir={}", value, dir);
            return 0.0;
        }
    }

    /**
     * 校验 NMEA 语句的 Checksum
     */
    private boolean validateChecksum(String nmea) {
        if (nmea == null) return false;

        int starIndex = nmea.indexOf('*');
        if (starIndex < 0 || starIndex + 3 > nmea.length()) {
            return false;
        }

        try {
            int calculated = 0;
            for (int i = 1; i < starIndex; i++) {
                calculated ^= (nmea.charAt(i) & 0xFF);
            }

            String expectedHex = String.format("%02X", calculated);
            String actualHex = nmea.substring(starIndex + 1, starIndex + 3).toUpperCase();

            return expectedHex.equals(actualHex);
        } catch (Exception e) {
            return false;
        }
    }

    /**
     * 安全解析整数
     */
    private int parsePositiveInt(String value) {
        if (value == null || value.trim().isEmpty()) return 0;
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return 0;
        }
    }

    /**
     * 安全解析浮点数
     */
    private double parseDouble(String value) {
        if (value == null || value.trim().isEmpty()) return 0.0;
        try {
            return Double.parseDouble(value.trim());
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    /**
     * 截断超长字符串用于日志输出
     */
    private String truncateForLog(String s) {
        if (s == null) return "null";
        return s.length() > 80 ? s.substring(0, 80) + "..." : s;
    }
}