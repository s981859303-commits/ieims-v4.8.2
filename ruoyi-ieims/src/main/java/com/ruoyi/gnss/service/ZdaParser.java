package com.ruoyi.gnss.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;

/**
 * ZDA 语句解析器
 *
 * 功能说明：
 * 1. 解析 GNZDA / GPZDA / BDZDA 语句，提取日期信息
 * 2. 校验 NMEA checksum，确保数据完整性
 * 3. 对异常语句安全失败，不影响主流程
 *
 * ZDA 语句格式：
 * $GNZDA,hhmmss.ss,dd,mm,yyyy,xx,yy*CC
 *
 * 字段说明：
 * - $GNZDA: 语句类型（GN=多系统, GP=GPS, BD=北斗）
 * - hhmmss.ss: UTC 时间
 * - dd: 日（01-31）
 * - mm: 月（01-12）
 * - yyyy: 年（如 2026）
 * - xx: 时区偏移小时（可选，部分接收机不输出）
 * - yy: 时区偏移分钟（可选，部分接收机不输出）
 * - *CC: 校验和（两位十六进制）
 *
 * @author GNSS Team
 * @date 2026-03-31
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
        // 使用 toUpperCase 统一处理大小写
        String upper = nmea.toUpperCase();
        return upper.startsWith(PREFIX_GNZDA) ||
                upper.startsWith(PREFIX_GPZDA) ||
                upper.startsWith(PREFIX_BDZDA);
    }

    /**
     * 解析 ZDA 语句，提取日期并格式化为指定格式的字符串
     *
     * @param nmea      原始 NMEA 语句
     * @param formatter 日期格式化器（如 DateTimeFormatter.ofPattern("yyyy-MM-dd")）
     * @return 格式化后的日期字符串，解析失败返回 null
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
                logger.debug("ZDA 字段数不足: 期望至少 {} 个, 实际 {} 个",
                        MIN_FIELDS, parts.length);
                return null;
            }

            // 4. 提取日期字段
            // parts[0] = $GNZDA
            // parts[1] = hhmmss.ss (UTC 时间)
            // parts[2] = dd (日)
            // parts[3] = mm (月)
            // parts[4] = yyyy (年)
            String dayStr = parts[2];
            String monthStr = parts[3];
            String yearStr = parts[4];

            // 5. 校验字段非空
            if (isEmptyField(dayStr) || isEmptyField(monthStr) || isEmptyField(yearStr)) {
                logger.debug("ZDA 日期字段为空: day='{}', month='{}', year='{}'",
                        dayStr, monthStr, yearStr);
                return null;
            }

            // 6. 解析数字
            int day = parsePositiveInt(dayStr);
            int month = parsePositiveInt(monthStr);
            int year = parsePositiveInt(yearStr);

            if (day < 0 || month < 0 || year < 0) {
                logger.debug("ZDA 日期字段包含非法数字: day='{}', month='{}', year='{}'",
                        dayStr, monthStr, yearStr);
                return null;
            }

            // 7. 范围校验
            if (year < MIN_YEAR || year > MAX_YEAR) {
                logger.debug("ZDA 年份超出合理范围: {} (有效范围: {}-{})",
                        year, MIN_YEAR, MAX_YEAR);
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

            // 8. 使用 LocalDate 进行严格日期校验（自动处理闰年、各月天数等）
            LocalDate date;
            try {
                date = LocalDate.of(year, month, day);
            } catch (Exception e) {
                logger.debug("ZDA 日期非法: {}-{}-{} ({})", year, month, day, e.getMessage());
                return null;
            }

            // 9. 格式化输出
            return date.format(formatter);

        } catch (Exception e) {
            // 捕获所有未预期的异常，确保不影响主流程
            logger.debug("ZDA 解析异常（不影响主流程）: {}", e.getMessage());
            return null;
        }
    }

    /**
     * 校验 NMEA checksum
     * <p>
     * NMEA checksum 计算规则：
     * - 对 $ 和 * 之间的所有字符（不含 $ 和 *）进行异或运算
     * - 结果为两位十六进制数，附加在 * 之后
     *
     * @param nmea 原始 NMEA 语句
     * @return true 如果 checksum 有效
     */
    public boolean validateChecksum(String nmea) {
        if (nmea == null || nmea.isEmpty()) {
            return false;
        }

        int starIndex = nmea.indexOf('*');
        if (starIndex < 0) {
            // 没有 * 号，无法校验
            return false;
        }

        // * 后面至少需要 2 个十六进制字符
        if (starIndex + 3 > nmea.length()) {
            return false;
        }

        try {
            // 计算 $ 和 * 之间字符的 XOR
            int calculated = 0;
            for (int i = 1; i < starIndex; i++) {
                calculated ^= (nmea.charAt(i) & 0xFF);
            }

            // 提取语句中的 checksum
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
     *
     * @return 解析结果，失败返回 -1
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
     * 截断字符串用于日志输出，避免日志过长
     */
    private String truncateForLog(String s) {
        if (s == null) return "null";
        return s.length() > 80 ? s.substring(0, 80) + "..." : s;
    }
}
