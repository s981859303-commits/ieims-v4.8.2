package com.ruoyi.gnss.service;

import com.ruoyi.gnss.domain.GsvSatelliteData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;

/**
 * GSV 语句解析器
 *
 * 功能说明：
 * 1. 解析 GPGSV（GPS）和 GBGSV（北斗）语句
 * 2. 提取卫星编号、仰角、方位角、信噪比
 * 3. 支持多语句组合解析（GSV 语句可能分多帧发送）
 *
 * GSV 语句格式：
 * $GPGSV,3,1,12,01,45,123,42,02,38,089,38,03,22,234,40,04,15,180,36*7A
 *
 * 字段说明：
 * - $GPGSV: 语句类型（GPGSV=GPS, GBGSV=北斗）
 * - 3: 总语句数
 * - 1: 当前语句序号
 * - 12: 可见卫星总数
 * - 01: 卫星PRN号
 * - 45: 仰角（度，0-90）
 * - 123: 方位角（度，0-360）
 * - 42: 信噪比（dB-Hz，空表示未跟踪）
 *
 * @author GNSS Team
 * @date 2026-03-25
 */
@Component
public class GsvParser {

    private static final Logger logger = LoggerFactory.getLogger(GsvParser.class);

    /** GPGSV 语句前缀（GPS） */
    private static final String PREFIX_GPGSV = "$GPGSV";

    /** GBGSV 语句前缀（北斗） */
    private static final String PREFIX_GBGSV = "$GBGSV";

    /** GNGSV 语句前缀（多系统混合） */
    private static final String PREFIX_GNGSV = "$GNGSV";

    /** 预期的每条 GSV 语句最大卫星数 */
    private static final int MAX_SATS_PER_SENTENCE = 4;

    /**
     * 解析 GSV 语句
     *
     * @param gsvLine GSV 语句
     * @return 卫星数据列表，解析失败返回空列表
     */
    public List<GsvSatelliteData> parseGsv(String gsvLine) {
        // 预分配容量
        List<GsvSatelliteData> result = new ArrayList<>(MAX_SATS_PER_SENTENCE);

        if (gsvLine == null || !gsvLine.startsWith("$")) {
            return result;
        }

        try {
            // 移除校验和（*hh）
            int starIndex = gsvLine.indexOf('*');
            String line = starIndex > 0 ? gsvLine.substring(0, starIndex) : gsvLine;
            String[] parts = line.split(",", -1);

            if (parts.length < 4) {
                return result;
            }

            // 解析语句头信息
            String sentenceType = parts[0];
            int totalSentences = parseInteger(parts[1], 0);
            int sentenceNumber = parseInteger(parts[2], 0);
            int totalSats = parseInteger(parts[3], 0);

            // 确定卫星系统
            String satSystem = determineSatSystem(sentenceType);
            if (satSystem == null) {
                return result;
            }

            // 解析卫星数据（每颗卫星占4个字段：PRN、仰角、方位角、信噪比）
            for (int i = 4; i + 3 < parts.length; i += 4) {
                GsvSatelliteData data = parseSatelliteData(parts, i, satSystem,
                        totalSentences, sentenceNumber, totalSats);
                if (data != null && data.hasValidData()) {
                    result.add(data);
                }
            }

        } catch (Exception e) {
            logger.error("解析 GSV 语句异常: {}", e.getMessage());
        }

        return result;
    }

    /**
     * 解析单颗卫星数据
     */
    private GsvSatelliteData parseSatelliteData(String[] parts, int startIndex,
                                                String satSystem, int totalSentences,
                                                int sentenceNumber, int totalSats) {

        GsvSatelliteData data = new GsvSatelliteData();
        data.setSatSystem(satSystem);
        data.setTotalSentences(totalSentences);
        data.setSentenceNumber(sentenceNumber);
        data.setTotalSats(totalSats);
        data.setTimestamp(System.currentTimeMillis());

        // PRN 号
        String prnStr = parts[startIndex];
        if (prnStr == null || prnStr.isEmpty()) {
            return null;
        }

        try {
            int prn = Integer.parseInt(prnStr);
            data.setPrn(prn);
            data.setSatNo(SatNoConverter.convert(satSystem, prn));
        } catch (NumberFormatException e) {
            return null;
        }

        // 仰角（度）
        data.setElevation(parseDoubleInRange(parts[startIndex + 1], 0, 90));

        // 方位角（度）
        data.setAzimuth(parseDoubleInRange(parts[startIndex + 2], 0, 360));

        // 信噪比（dB-Hz）
        data.setSnr(parseDoubleInRange(parts[startIndex + 3], 0, 99));

        return data;
    }

    /**
     * 根据 GSV 语句类型确定卫星系统
     */
    private String determineSatSystem(String sentenceType) {
        if (sentenceType == null) {
            return null;
        }

        if (sentenceType.startsWith(PREFIX_GPGSV)) {
            return SatNoConverter.SYS_GPS;
        } else if (sentenceType.startsWith(PREFIX_GBGSV)) {
            return SatNoConverter.SYS_BDS;
        } else if (sentenceType.startsWith(PREFIX_GNGSV)) {
            // GNGSV 是多系统混合，需要根据卫星PRN号判断
            return null;
        }

        return null;
    }

    /**
     * 安全解析整数
     */
    private int parseInteger(String value, int defaultValue) {
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    /**
     * 解析 Double 并检查范围
     */
    private Double parseDoubleInRange(String value, double min, double max) {
        if (value == null || value.isEmpty()) {
            return null;
        }
        try {
            double d = Double.parseDouble(value);
            if (d >= min && d <= max) {
                return d;
            }
            return null;
        } catch (NumberFormatException e) {
            return null;
        }
    }

    /**
     * 判断是否为 GSV 语句
     */
    public boolean isGsvSentence(String nmea) {
        if (nmea == null) {
            return false;
        }
        return nmea.startsWith(PREFIX_GPGSV) ||
                nmea.startsWith(PREFIX_GBGSV) ||
                nmea.startsWith(PREFIX_GNGSV);
    }

    /**
     * 获取 GSV 语句类型
     *
     * @return "GPS" / "BDS" / "MIXED" / null
     */
    public String getGsvType(String nmea) {
        if (nmea == null) {
            return null;
        }
        if (nmea.startsWith(PREFIX_GPGSV)) {
            return "GPS";
        } else if (nmea.startsWith(PREFIX_GBGSV)) {
            return "BDS";
        } else if (nmea.startsWith(PREFIX_GNGSV)) {
            return "MIXED";
        }
        return null;
    }
}
