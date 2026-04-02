package com.ruoyi.gnss.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

/**
 * ZDA 语句解析器
 *
 * 功能说明：
 * 1. 解析 GNZDA / GPZDA / BDZDA 语句，提取日期信息
 * 2. 校验 NMEA checksum，确保数据完整性
 * 3. 自动处理 GNSS UTC 时间到本地时区的转换，防止跨天边界切分错误
 * 4. 对异常语句安全失败，不影响主流程
 *
 * ZDA 语句格式：
 * $GNZDA,hhmmss.ss,dd,mm,yyyy,xx,yy*CC
 */
@Component
public class ZdaParser {

    private static final Logger logger = LoggerFactory.getLogger(ZdaParser.class);

    /** GNZDA 语句前缀（多系统混合） */
    private static final String PREFIX_GNZDA = "$GNZDA";

    /** GPZDA 语句前缀（GPS） */
    private static final String PREFIX_GPZDA = "$GPZDA";

    /** BDZDA 语句前缀（北斗） */
    private static final String PREFIX_BDZDA = "$BDZDA";

    /** ZDA 语句最小字段数（不含 checksum）: $GNZDA,time,dd,mm,yyyy = 5 个逗号分隔字段 */
    private static final int MIN_FIELDS = 5;

    /** 年份合理范围下限 */
    private static final int MIN_YEAR = 1980;

    /** 年份合理范围上限 */
    private static final int MAX_YEAR = 2100;

    /** NMEA 语句 checksum 标识符 '*' 的最小合法索引（$TALKER= 至少 6 个字符后才能有 *） */
    private static final int MIN_STAR_INDEX = 5;

    /**
     * 判断是否为 ZDA 语句
     *
     * @param nmea 原始 NMEA 语句
     * @return true 如果是 ZDA 语句
     */
    public boolean isZdaSentence(String nmea) {
        if (nmea == null || nmea.isEmpty()) {
            return false;
        }
        String upper = nmea.toUpperCase();
        return upper.startsWith(PREFIX_GNZDA) ||
                upper.startsWith(PREFIX_GPZDA) ||
                upper.startsWith(PREFIX_BDZDA);
    }

    /**
     * 解析 ZDA 语句，将 UTC 时间转换为本地时区后，提取日期并格式化
     *
     * @param nmea      原始 NMEA 语句
     * @param formatter 日期格式化器（如 DateTimeFormatter.ofPattern("yyyy-MM-dd")）
     * @return 转换时区并格式化后的本地日期字符串，解析失败返回 null
     */
    public String parseDate(String nmea, DateTimeFormatter formatter) {
        if (nmea == null || nmea.isEmpty() || formatter == null) {
            return null;
        }

        try {
            // 1. 校验 checksum
            if (!validateChecksum(nmea)) {
                logger.debug("ZDA checksum 校验失败: {}", truncateForLog(nmea));
                return null;
            }

            // 2. 提取语句体（去掉 *CC 部分）
            int starIndex = nmea.indexOf('*');
            String body = (starIndex > 0) ? nmea.substring(0, starIndex) : nmea;

            // 3. 按逗号分割字段
            String[] parts = body.split(",", -1);

            if (parts.length < MIN_FIELDS) {
                logger.debug("ZDA 字段数不足: 期望至少 {} 个, 实际 {} 个", MIN_FIELDS, parts.length);
                return null;
            }

            // 4. 提取时间与日期字段
            String timeStr = parts[1]; // hhmmss.ss
            String dayStr = parts[2];  // dd
            String monthStr = parts[3]; // mm
            String yearStr = parts[4]; // yyyy

            // 5. 校验日期字段非空
            if (isEmptyField(dayStr) || isEmptyField(monthStr) || isEmptyField(yearStr)) {
                logger.debug("ZDA 日期字段为空: day='{}', month='{}', year='{}'", dayStr, monthStr, yearStr);
                return null;
            }

            // 6. 解析年月日数字
            int day = parsePositiveInt(dayStr);
            int month = parsePositiveInt(monthStr);
            int year = parsePositiveInt(yearStr);

            if (day < 0 || month < 0 || year < 0) {
                logger.debug("ZDA 日期字段包含非法数字: day='{}', month='{}', year='{}'", dayStr, monthStr, yearStr);
                return null;
            }

            // 7. 范围校验
            if (year < MIN_YEAR || year > MAX_YEAR) {
                logger.debug("ZDA 年份超出合理范围: {} (有效范围: {}-{})", year, MIN_YEAR, MAX_YEAR);
                return null;
            }
            if (month < 1 || month > 12) {
                logger.debug("ZDA 月份超出范围: {} (有效范围: 1-12)", month);
                return null;
            }
            if (day < 1 || day > 31) {
                logger.debug("ZDA 日期超出范围: {} (有效范围: 1-31)", day);
                return null;
            }

            // 8. 解析时分秒 (UTC时间)
            int hour = 0, minute = 0, second = 0;
            if (!isEmptyField(timeStr) && timeStr.length() >= 6) {
                try {
                    hour = Integer.parseInt(timeStr.substring(0, 2));
                    minute = Integer.parseInt(timeStr.substring(2, 4));
                    second = Integer.parseInt(timeStr.substring(4, 6));
                    
                    if (hour < 0 || hour > 23 || minute < 0 || minute > 59 || second < 0 || second > 59) {
                        logger.debug("ZDA 时间字段超出有效范围: hour={}, minute={}, second={} (使用默认值 00:00:00)", hour, minute, second);
                        hour = 0;
                        minute = 0;
                        second = 0;
                    }
                } catch (NumberFormatException e) {
                    logger.debug("ZDA 时间字段解析失败，使用默认值 00:00:00: {}", timeStr);
                }
            }

            // 9. 核心修复：组装 UTC 绝对时间并转换为本地时区
            LocalDate utcDate;
            LocalTime utcTime;
            try {
                utcDate = LocalDate.of(year, month, day);
                utcTime = LocalTime.of(hour, minute, second);

                // 组合为标准 UTC 时间
                ZonedDateTime utcZonedDateTime = ZonedDateTime.of(utcDate, utcTime, ZoneOffset.UTC);

                // 转换为系统所在时区的本地时间 (如系统在东八区/东九区，会自动加上 8/9 小时并处理进位)
                ZonedDateTime localZonedDateTime = utcZonedDateTime.withZoneSameInstant(ZoneId.systemDefault());

                // 10. 基于转换后的本地时间进行格式化输出
                return localZonedDateTime.format(formatter);

            } catch (Exception e) {
                logger.debug("ZDA 日期/时间重组异常: {}-{}-{} {}:{}:{} ({})",
                        year, month, day, hour, minute, second, e.getMessage());
                return null;
            }

        } catch (Exception e) {
            logger.debug("ZDA 解析异常（不影响主流程）: {}", e.getMessage());
            return null;
        }
    }

    /**
     * 校验 NMEA checksum
     */
    public boolean validateChecksum(String nmea) {
        if (nmea == null || nmea.isEmpty()) {
            return false;
        }

        int starIndex = nmea.indexOf('*');
        if (starIndex < MIN_STAR_INDEX) {
            return false;
        }

        if (starIndex + 3 > nmea.length()) {
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
     * 判断字段是否为空或仅含空白
     */
    private boolean isEmptyField(String field) {
        return field == null || field.trim().isEmpty();
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
}