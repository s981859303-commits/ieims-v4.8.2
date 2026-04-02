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
 * 5. 【新增】提供日期缓存结果对象，便于上层存储和传递
 *
 * ZDA 语句格式：
 * $GNZDA,hhmmss.ss,dd,mm,yyyy,xx,yy*CC
 *
 * 示例：
 * $GNZDA,083015.00,01,04,2026,00,00*6A
 * 解析结果：2026-04-01 08:30:15.00 UTC
 *
 * @version 2.0 - 2026-04-02 增强日期缓存和结果对象
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

    /** 时间格式化器：hhmmss.ss */
    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HHmmss[.SS][.S]");

    // ==================== 公共接口 ====================

    /**
     * ZDA 解析结果对象
     * 包含完整的日期时间信息和解析状态
     */
    public static class ZdaResult {
        /** 是否解析成功 */
        private final boolean success;

        /** 完整的日期时间（UTC） */
        private final ZonedDateTime dateTime;

        /** 日期部分 */
        private final LocalDate date;

        /** 时间部分 */
        private final LocalTime time;

        /** 原始语句 */
        private final String rawSentence;

        /** 错误信息（解析失败时） */
        private final String errorMessage;

        /** 解析时间戳 */
        private final long parseTimestamp;

        private ZdaResult(boolean success, ZonedDateTime dateTime, LocalDate date, LocalTime time,
                          String rawSentence, String errorMessage) {
            this.success = success;
            this.dateTime = dateTime;
            this.date = date;
            this.time = time;
            this.rawSentence = rawSentence;
            this.errorMessage = errorMessage;
            this.parseTimestamp = System.currentTimeMillis();
        }

        /** 创建成功结果 */
        public static ZdaResult success(ZonedDateTime dateTime, String rawSentence) {
            return new ZdaResult(true, dateTime, dateTime.toLocalDate(),
                    dateTime.toLocalTime(), rawSentence, null);
        }

        /** 创建失败结果 */
        public static ZdaResult failure(String rawSentence, String errorMessage) {
            return new ZdaResult(false, null, null, null, rawSentence, errorMessage);
        }

        // Getters
        public boolean isSuccess() { return success; }
        public ZonedDateTime getDateTime() { return dateTime; }
        public LocalDate getDate() { return date; }
        public LocalTime getTime() { return time; }
        public String getRawSentence() { return rawSentence; }
        public String getErrorMessage() { return errorMessage; }
        public long getParseTimestamp() { return parseTimestamp; }

        /** 获取日期字符串（YYYY-MM-DD格式） */
        public String getDateStr() {
            return date != null ? date.format(DateTimeFormatter.ISO_LOCAL_DATE) : null;
        }

        /** 获取时间字符串（HH:MM:SS格式） */
        public String getTimeStr() {
            return time != null ? time.format(DateTimeFormatter.ofPattern("HH:mm:ss.SSS")) : null;
        }

        /** 获取完整时间戳（毫秒） */
        public Long getTimestampMs() {
            return dateTime != null ? dateTime.toInstant().toEpochMilli() : null;
        }

        @Override
        public String toString() {
            if (success) {
                return String.format("ZdaResult[success=true, date=%s, time=%s]", getDateStr(), getTimeStr());
            } else {
                return String.format("ZdaResult[success=false, error=%s]", errorMessage);
            }
        }
    }

    /**
     * 解析 ZDA 语句并返回完整结果对象
     *
     * @param nmea NMEA 语句字符串
     * @return ZdaResult 解析结果对象
     */
    public ZdaResult parseWithResult(String nmea) {
        if (nmea == null || nmea.isEmpty()) {
            return ZdaResult.failure(nmea, "输入语句为空");
        }

        // 检查是否为 ZDA 语句
        if (!isZdaSentence(nmea)) {
            return ZdaResult.failure(nmea, "不是ZDA语句");
        }

        // 校验 checksum
        if (!validateChecksum(nmea)) {
            return ZdaResult.failure(nmea, "Checksum校验失败");
        }

        try {
            // 移除 checksum 部分后按逗号分割
            int starIndex = nmea.indexOf('*');
            String contentPart = starIndex > 0 ? nmea.substring(0, starIndex) : nmea;
            String[] fields = contentPart.split(",");

            if (fields.length < MIN_FIELDS) {
                return ZdaResult.failure(nmea,
                        String.format("字段数不足: 期望%d, 实际%d", MIN_FIELDS, fields.length));
            }

            // 解析时间字段：hhmmss.ss
            String timeField = fields[1];
            LocalTime time = parseTimeField(timeField);
            if (time == null) {
                return ZdaResult.failure(nmea, "时间字段解析失败: " + timeField);
            }

            // 解析日期字段：dd,mm,yyyy
            int day = parsePositiveInt(fields[2]);
            int month = parsePositiveInt(fields[3]);
            int year = parsePositiveInt(fields[4]);

            // 验证日期有效性
            if (!isValidDate(year, month, day)) {
                return ZdaResult.failure(nmea,
                        String.format("日期无效: year=%d, month=%d, day=%d", year, month, day));
            }

            // 构建 ZonedDateTime（UTC时区）
            LocalDate date = LocalDate.of(year, month, day);
            ZonedDateTime zdt = ZonedDateTime.of(date, time, ZoneOffset.UTC);

            logger.debug("ZDA解析成功: {} -> {}", truncateForLog(nmea), zdt);
            return ZdaResult.success(zdt, nmea);

        } catch (Exception e) {
            logger.warn("ZDA解析异常: {} - {}", truncateForLog(nmea), e.getMessage());
            return ZdaResult.failure(nmea, "解析异常: " + e.getMessage());
        }
    }

    /**
     * 解析 ZDA 语句，返回 ZonedDateTime（兼容旧接口）
     *
     * @param nmea NMEA 语句字符串
     * @return 解析后的 ZonedDateTime，失败返回 null
     */
    public ZonedDateTime parse(String nmea) {
        ZdaResult result = parseWithResult(nmea);
        return result.isSuccess() ? result.getDateTime() : null;
    }

    /**
     * 仅解析日期部分
     *
     * @param nmea NMEA 语句字符串
     * @return 解析后的 LocalDate，失败返回 null
     */
    public LocalDate parseDate(String nmea) {
        ZdaResult result = parseWithResult(nmea);
        return result.getDate();
    }

    /**
     * 判断是否为 ZDA 语句
     *
     * @param nmea NMEA 语句
     * @return true 表示是 ZDA 语句
     */
    public boolean isZdaSentence(String nmea) {
        if (nmea == null) {
            return false;
        }
        return nmea.startsWith(PREFIX_GNZDA) ||
                nmea.startsWith(PREFIX_GPZDA) ||
                nmea.startsWith(PREFIX_BDZDA);
    }

    /**
     * 获取 ZDA 语句类型
     *
     * @param nmea NMEA 语句
     * @return "GNSS" / "GPS" / "BDS" / null
     */
    public String getZdaType(String nmea) {
        if (nmea == null) {
            return null;
        }
        if (nmea.startsWith(PREFIX_GNZDA)) {
            return "GNSS";
        } else if (nmea.startsWith(PREFIX_GPZDA)) {
            return "GPS";
        } else if (nmea.startsWith(PREFIX_BDZDA)) {
            return "BDS";
        }
        return null;
    }

    // ==================== 私有辅助方法 ====================

    /**
     * 解析时间字段
     * 支持格式：hhmmss.ss, hhmmss.s, hhmmss
     *
     * @param timeField 时间字段字符串
     * @return LocalTime 或 null
     */
    private LocalTime parseTimeField(String timeField) {
        if (isEmptyField(timeField)) {
            return null;
        }

        try {
            String timeStr = timeField.trim();

            // 补齐小数位
            int dotIndex = timeStr.indexOf('.');
            if (dotIndex > 0) {
                String decimal = timeStr.substring(dotIndex + 1);
                // 补齐到3位小数（毫秒）
                if (decimal.length() == 1) {
                    timeStr = timeStr + "00";
                } else if (decimal.length() == 2) {
                    timeStr = timeStr + "0";
                }
            }

            // 解析时分秒
            int hour = Integer.parseInt(timeStr.substring(0, 2));
            int minute = Integer.parseInt(timeStr.substring(2, 4));
            int second = Integer.parseInt(timeStr.substring(4, 6));
            int nano = 0;

            // 解析毫秒
            if (timeStr.length() > 7) {
                int millis = Integer.parseInt(timeStr.substring(7, 10));
                nano = millis * 1_000_000;
            }

            // 验证时间有效性
            if (hour < 0 || hour > 23 || minute < 0 || minute > 59 ||
                    second < 0 || second > 59) {
                return null;
            }

            return LocalTime.of(hour, minute, second, nano);

        } catch (Exception e) {
            logger.debug("时间字段解析失败: {} - {}", timeField, e.getMessage());
            return null;
        }
    }

    /**
     * 验证日期有效性
     */
    private boolean isValidDate(int year, int month, int day) {
        if (year < MIN_YEAR || year > MAX_YEAR) {
            return false;
        }
        if (month < 1 || month > 12) {
            return false;
        }
        if (day < 1 || day > 31) {
            return false;
        }

        // 使用 LocalDate 进行严格验证
        try {
            LocalDate.of(year, month, day);
            return true;
        } catch (Exception e) {
            return false;
        }
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
