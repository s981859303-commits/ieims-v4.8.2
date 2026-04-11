package com.ruoyi.gnss.service;

import com.ruoyi.gnss.domain.GsvSatelliteData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.beans.factory.annotation.Value;
/**
 * GSV 语句解析器
 *
 * 功能：
 * 1. 解析 GPGSV / GBGSV / GLGSV / GAGSV 语句
 * 2. 提取卫星仰角、方位角、信噪比
 * 3. 校验 NMEA checksum
 * 4. 支持多语句组包（GSV可能分多条发送）
 * 5. 【新增】关联ZDA日期信息
 *
 * GSV 语句格式：
 * $xxGSV,totalMsg,msgNum,satInView,[prn,elev,azim,snr]x4*CC
 *
 * 示例：
 * $GPGSV,3,1,12,01,45,123,42,02,38,089,38,03,22,234,40,04,15,180,36*7A
 * $GPGSV,3,2,12,05,52,270,44,06,29,042,40,07,12,315,35,09,48,135,41*7B
 * $GPGSV,3,3,12,10,35,195,39,12,28,060,37,13,41,300,43,14,20,165,38*7C
 *
 * 【重构说明】
 * 1. 新增日期关联方法
 * 2. 新增多语句组包缓存
 * 3. 新增数据有效性校验
 * 4. 修复多线程安全问题
 *
 */
@Component
public class GsvParser {

    @Value("${gnss.gsv.groupExpireMs:5000}")
    private long gsvGroupExpireMs;

    // 使用 fixedRateString 支持从配置文件读取定时任务频率
    @Scheduled(fixedRateString = "${gnss.gsv.cleanupIntervalMs:5000}")
    public void scheduledCleanup() {
        cleanupExpiredCache(); // 清理过期缓存
    }
    private static final Logger logger = LoggerFactory.getLogger(GsvParser.class);

    // ==================== 常量定义 ====================

    /** GSV 语句前缀列表 */
    private static final String[] GSV_PREFIXES = {
            "$GPGSV",  // GPS
            "$GLGSV",  // GLONASS
            "$GAGSV",  // Galileo
            "$GBGSV",  // BDS (BeiDou)
            "$BDGSV",  // BDS (BeiDou, alternative)
            "$QZGSV",  // QZSS
            "$IGSV",   // IRNSS
            "$GNGSV"   // Multi-GNSS
    };

    /** 每条GSV语句最多包含的卫星数 */
    private static final int SATS_PER_GSV = 4;

    /** GSV语句最小字段数 */
    private static final int MIN_FIELDS = 4;

    /** GSV组包缓存过期时间（毫秒） */
    private static final long GSV_GROUP_EXPIRE_MS = 5000;

    // ==================== 状态缓存 ====================

    /** GSV组包缓存：key = stationId_sentenceType, value = GsvGroupCache */
    private final ConcurrentHashMap<String, GsvGroupCache> gsvGroupCache = new ConcurrentHashMap<>();

    // ==================== 公共接口 ====================

    /**
     * 判断是否为 GSV 语句
     *
     * @param nmea NMEA 语句
     * @return true 表示是 GSV 语句
     */
    public boolean isGsvSentence(String nmea) {
        if (nmea == null || nmea.isEmpty()) {
            return false;
        }
        for (String prefix : GSV_PREFIXES) {
            if (nmea.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 获取 GSV 语句类型
     *
     * @param nmea NMEA 语句
     * @return 卫星系统类型：GPS / GLONASS / GALILEO / BDS / QZSS / IRNSS / GNSS / null
     */
    public String getGsvType(String nmea) {
        if (nmea == null) {
            return null;
        }
        if (nmea.startsWith("$GPGSV")) return "GPS";
        if (nmea.startsWith("$GLGSV")) return "GLONASS";
        if (nmea.startsWith("$GAGSV")) return "GALILEO";
        if (nmea.startsWith("$GBGSV") || nmea.startsWith("$BDGSV")) return "BDS";
        if (nmea.startsWith("$QZGSV")) return "QZSS";
        if (nmea.startsWith("$IGSV")) return "IRNSS";
        if (nmea.startsWith("$GNGSV")) return "GNSS";
        return null;
    }

    /**
     * 解析 GSV 语句（基础方法）
     *
     * @param nmea NMEA 语句
     * @return 卫星数据列表，解析失败返回空列表
     */
    public List<GsvSatelliteData> parse(String nmea) {
        return parse(nmea, null, null);
    }

    /**
     * 解析 GSV 语句（带日期关联）
     *
     * @param nmea       NMEA 语句
     * @param obsDate    观测日期（来自ZDA）
     * @param epochTime  历元时间
     * @return 卫星数据列表，解析失败返回空列表
     */
    public List<GsvSatelliteData> parse(String nmea, LocalDate obsDate, Long epochTime) {
        if (!isGsvSentence(nmea)) {
            return new ArrayList<>();
        }

        // 校验 checksum
        if (!validateChecksum(nmea)) {
            logger.warn("GSV checksum校验失败: {}", truncateForLog(nmea));
            return new ArrayList<>();
        }

        try {
            // 移除 checksum 部分后按逗号分割
            int starIndex = nmea.indexOf('*');
            String contentPart = starIndex > 0 ? nmea.substring(0, starIndex) : nmea;
            String[] fields = contentPart.split(",");

            if (fields.length < MIN_FIELDS) {
                logger.warn("GSV字段数不足: {} < {}", fields.length, MIN_FIELDS);
                return new ArrayList<>();
            }

            // 解析头部信息
            String sentenceType = fields[0].substring(1); // 去掉 $
            int totalMsg = parsePositiveInt(fields[1]);
            int msgNum = parsePositiveInt(fields[2]);
            int satInView = parsePositiveInt(fields[3]);

            if (totalMsg <= 0 || msgNum <= 0) {
                logger.warn("GSV消息编号无效: totalMsg={}, msgNum={}", totalMsg, msgNum);
                return new ArrayList<>();
            }

            // 解析卫星数据（每条GSV最多4颗卫星）
            List<GsvSatelliteData> satellites = new ArrayList<>();
            int satStartIndex = 4;

            while (satStartIndex + 3 < fields.length) {
                GsvSatelliteData sat = parseSatellite(fields, satStartIndex, sentenceType);
                if (sat != null && sat.isValid()) {
                    // 设置日期和时间
                    sat.setObservationDate(obsDate);
                    sat.setEpochTime(epochTime);
                    if (epochTime != null) {
                        long msInDay = epochTime % (24 * 60 * 60 * 1000);
                        sat.setObservationTime(LocalTime.ofNanoOfDay(msInDay * 1_000_000));
                    }
                    sat.setTotalMsg(totalMsg);
                    sat.setMsgNum(msgNum);
                    sat.setSatInView(satInView);
                    sat.setRawSentence(nmea);
                    sat.calculateSatUniqueKey();
                    satellites.add(sat);
                }
                satStartIndex += 4;
            }

            logger.debug("GSV解析成功: type={}, msgNum={}/{}, sats={}",
                    sentenceType, msgNum, totalMsg, satellites.size());

            return satellites;

        } catch (Exception e) {
            logger.error("GSV解析异常: {} - {}", truncateForLog(nmea), e.getMessage());
            return new ArrayList<>();
        }
    }

    /**
     * 解析 GSV 语句并支持组包（多语句合并）
     *
     * @param stationId  站点ID
     * @param nmea       NMEA 语句
     * @param obsDate    观测日期
     * @param epochTime  历元时间
     * @return 完整的卫星数据列表（如果组包完成），否则返回当前解析的数据
     */
    public List<GsvSatelliteData> parseWithGrouping(String stationId, String nmea,
                                                    LocalDate obsDate, Long epochTime) {
        List<GsvSatelliteData> currentSats = parse(nmea, obsDate, epochTime);
        if (currentSats.isEmpty()) {
            return currentSats;
        }

        // 获取头部信息
        int starIndex = nmea.indexOf('*');
        String contentPart = starIndex > 0 ? nmea.substring(0, starIndex) : nmea;
        String[] fields = contentPart.split(",");

        int totalMsg = parsePositiveInt(fields[1]);
        int msgNum = parsePositiveInt(fields[2]);
        String sentenceType = fields[0].substring(1);

        // 如果只有一条消息，直接返回
        if (totalMsg == 1) {
            return currentSats;
        }

        // 组包处理
        String cacheKey = stationId + "_" + sentenceType + "_" + (epochTime != null ? epochTime : System.currentTimeMillis());

        GsvGroupCache cache = gsvGroupCache.computeIfAbsent(cacheKey, k -> new GsvGroupCache());
        cache.totalMsg = totalMsg;
        cache.receivedMsgs.add(msgNum);
        cache.allSatellites.addAll(currentSats);
        cache.lastUpdateTime = System.currentTimeMillis();

        // 检查是否收齐所有消息
        if (cache.receivedMsgs.size() >= totalMsg) {
            List<GsvSatelliteData> result = new ArrayList<>(cache.allSatellites);
            gsvGroupCache.remove(cacheKey);
            logger.debug("GSV组包完成: type={}, totalSats={}", sentenceType, result.size());
            return result;
        }

        // 未收齐，返回空列表（等待后续消息）
        return new ArrayList<>();
    }

    /**
     * 清理过期的组包缓存
     */
    public void cleanupExpiredCache() {
        long now = System.currentTimeMillis();
        gsvGroupCache.entrySet().removeIf(entry ->
                (now - entry.getValue().lastUpdateTime) > this.gsvGroupExpireMs);
    }

    // ==================== 私有方法 ====================

    /**
     * 解析单个卫星数据
     */
    private GsvSatelliteData parseSatellite(String[] fields, int startIndex, String sentenceType) {
        try {
            String prnStr = fields[startIndex];
            String elevStr = fields[startIndex + 1];
            String azimStr = fields[startIndex + 2];
            String snrStr = (startIndex + 3 < fields.length) ? fields[startIndex + 3] : "";

            // PRN号必须有效
            if (prnStr == null || prnStr.isEmpty()) {
                return null;
            }

            int prn = parsePositiveInt(prnStr);
            if (prn <= 0) {
                return null;
            }

            GsvSatelliteData sat = new GsvSatelliteData();
            sat.setPrn(prn);

            // 根据语句类型确定卫星编号前缀
            String satNo = convertToSatNo(sentenceType, prn);
            sat.setSatNo(satNo);

            // 解析仰角
            if (elevStr != null && !elevStr.isEmpty()) {
                try {
                    sat.setElevation(Double.parseDouble(elevStr));
                } catch (NumberFormatException e) {
                    // 仰角可能为空
                }
            }

            // 解析方位角
            if (azimStr != null && !azimStr.isEmpty()) {
                try {
                    sat.setAzimuth(Double.parseDouble(azimStr));
                } catch (NumberFormatException e) {
                    // 方位角可能为空
                }
            }

            // 解析信噪比
            if (snrStr != null && !snrStr.isEmpty()) {
                try {
                    sat.setSnr(Double.parseDouble(snrStr));
                } catch (NumberFormatException e) {
                    // 信噪比可能为空
                }
            }

            // 校验数据有效性
            sat.validate();

            return sat;

        } catch (Exception e) {
            logger.debug("解析卫星数据失败: {}", e.getMessage());
            return null;
        }
    }

    /**
     * 将PRN转换为统一格式的卫星编号
     */
    private String convertToSatNo(String sentenceType, int prn) {
        // 根据语句类型确定前缀
        String prefix;
        if (sentenceType.startsWith("GP")) {
            prefix = "G";
        } else if (sentenceType.startsWith("GL")) {
            prefix = "R";
        } else if (sentenceType.startsWith("GA")) {
            prefix = "E";
        } else if (sentenceType.startsWith("GB") || sentenceType.startsWith("BD")) {
            prefix = "C";
        } else if (sentenceType.startsWith("QZ")) {
            prefix = "J";
        } else if (sentenceType.startsWith("I")) {
            prefix = "I";
        } else {
            // GNGSV 需要根据PRN范围判断
            // GPS: 1-32, GLONASS: 65-96, Galileo: 1-36, BDS: 1-63
            // 这里简化处理，使用G前缀
            prefix = "G";
        }

        return String.format("%s%02d", prefix, prn);
    }

    /**
     * 校验 NMEA checksum
     */
    private boolean validateChecksum(String nmea) {
        if (nmea == null) {
            return false;
        }

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
     * 安全解析正整数
     */
    private int parsePositiveInt(String value) {
        if (value == null || value.isEmpty()) {
            return -1;
        }
        try {
            return Integer.parseInt(value.trim());
        } catch (NumberFormatException e) {
            return -1;
        }
    }

    /**
     * 截断字符串用于日志输出
     */
    private String truncateForLog(String s) {
        if (s == null) return "null";
        return s.length() > 80 ? s.substring(0, 80) + "..." : s;
    }

    // ==================== 内部类 ====================

    /**
     * GSV 组包缓存
     */
    private static class GsvGroupCache {
        int totalMsg;
        List<Integer> receivedMsgs = new ArrayList<>();
        List<GsvSatelliteData> allSatellites = new ArrayList<>();
        long lastUpdateTime;
    }
}
