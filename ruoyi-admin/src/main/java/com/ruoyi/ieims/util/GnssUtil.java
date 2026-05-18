package com.ruoyi.ieims.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.ruoyi.user.comm.core.tdengine.TDengineUtil;

import java.util.*;
import java.util.stream.Collectors;

/**
 * GNSS工具类 - S4闪烁指数计算
 *
 * @author guet_developer01
 * @date 2026-04-27
 */
@Component
public class GnssUtil {

    private static final Logger log = LoggerFactory.getLogger(GnssUtil.class);

    /** 1分钟 = 60000毫秒 */
    private static final long ONE_MINUTE_MS = 60000L;

    /** SNR转强度时的最小值（避免除零） */
    private static final double MIN_SNR = 0.01;

    @Autowired
    private TDengineUtil tdengineUtil;

    /**
     * S4指数输入参数内部类
     */
    public static class S4Input {
        /** SNR时间序列 (dB-Hz) */
        private List<Double> snrSeries;
        /** 时间戳序列 (ms) */
        private List<Long> timestamps;
        /** 卫星编号 */
        private String satNo;
        /** 站点ID */
        private String stationId;

        public List<Double> getSnrSeries() {
            return snrSeries;
        }

        public void setSnrSeries(List<Double> snrSeries) {
            this.snrSeries = snrSeries;
        }

        public List<Long> getTimestamps() {
            return timestamps;
        }

        public void setTimestamps(List<Long> timestamps) {
            this.timestamps = timestamps;
        }

        public String getSatNo() {
            return satNo;
        }

        public void setSatNo(String satNo) {
            this.satNo = satNo;
        }

        public String getStationId() {
            return stationId;
        }

        public void setStationId(String stationId) {
            this.stationId = stationId;
        }

        /**
         * 检查输入数据是否有效
         */
        public boolean isValid() {
            return snrSeries != null && !snrSeries.isEmpty()
                    && timestamps != null && !timestamps.isEmpty()
                    && snrSeries.size() == timestamps.size()
                    && satNo != null && !satNo.trim().isEmpty()
                    && stationId != null && !stationId.trim().isEmpty();
        }
    }

    /**
     * S4指数输出结果
     */
    public static class S4Result {
        /** S4指数值 (0-1) */
        private double s4Index;
        /** 平均强度 */
        private double intensityMean;
        /** 强度标准差 */
        private double intensityStd;
        /** 计算窗口开始时间 */
        private long startTime;
        /** 计算窗口结束时间 */
        private long endTime;
        /** 样本数量 */
        private int sampleCount;
        /** 卫星编号 */
        private String satNo;
        /** 站点ID */
        private String stationId;

        public double getS4Index() {
            return s4Index;
        }

        public void setS4Index(double s4Index) {
            this.s4Index = s4Index;
        }

        public double getIntensityMean() {
            return intensityMean;
        }

        public void setIntensityMean(double intensityMean) {
            this.intensityMean = intensityMean;
        }

        public double getIntensityStd() {
            return intensityStd;
        }

        public void setIntensityStd(double intensityStd) {
            this.intensityStd = intensityStd;
        }

        public long getStartTime() {
            return startTime;
        }

        public void setStartTime(long startTime) {
            this.startTime = startTime;
        }

        public long getEndTime() {
            return endTime;
        }

        public void setEndTime(long endTime) {
            this.endTime = endTime;
        }

        public int getSampleCount() {
            return sampleCount;
        }

        public void setSampleCount(int sampleCount) {
            this.sampleCount = sampleCount;
        }

        public String getSatNo() {
            return satNo;
        }

        public void setSatNo(String satNo) {
            this.satNo = satNo;
        }

        public String getStationId() {
            return stationId;
        }

        public void setStationId(String stationId) {
            this.stationId = stationId;
        }

        @Override
        public String toString() {
            return "S4Result{" +
                    "s4Index=" + s4Index +
                    ", intensityMean=" + intensityMean +
                    ", intensityStd=" + intensityStd +
                    ", startTime=" + startTime +
                    ", endTime=" + endTime +
                    ", sampleCount=" + sampleCount +
                    ", satNo='" + satNo + '\'' +
                    ", stationId='" + stationId + '\'' +
                    '}';
        }
    }

    // ==================== 公开API方法 ====================

    /**
     * 计算所有站点和卫星组合的S4指数
     * 从TDengine获取最近1分钟数据，按station_id和sat_no分组计算
     *
     * @return S4指数计算结果列表，按station_id、sat_no升序排列
     */
    public List<S4Result> calculateS4() {
        log.info("开始计算S4闪烁指数");
        long startOverall = System.currentTimeMillis();

        // 1. 查询最近1分钟内的所有不重复的(station_id, sat_no)组合
        List<StationSatellitePair> pairs = queryDistinctStationSatellitePairs();
        if (pairs == null || pairs.isEmpty()) {
            log.warn("未查询到任何站点-卫星数据");
            return Collections.emptyList();
        }
        log.info("查询到{}个站点-卫星组合", pairs.size());

        // 2. 按station_id和sat_no升序排序
        pairs.sort((p1, p2) -> {
            int cmp = p1.stationId.compareTo(p2.stationId);
            if (cmp != 0) {
                return cmp;
            }
            return p1.satNo.compareTo(p2.satNo);
        });

        // 3. 批量查询所有组合的SNR数据（一次查询获取所有数据）
        List<S4Input> allInputs = batchQuerySnrDataForPairs(pairs);

        // 4. 逐个计算S4指数
        List<S4Result> results = new ArrayList<>();
        for (S4Input input : allInputs) {
            if (input != null && input.isValid()) {
                S4Result result = calculateSingleS4(input);
                if (result != null) {
                    results.add(result);
                }
            }
        }

        long endOverall = System.currentTimeMillis();
        log.info("S4指数计算完成，共{}个有效结果，耗时{}ms", results.size(), (endOverall - startOverall));

        return results;
    }

    /**
     * 计算指定站点和卫星的S4指数
     *
     * @param stationId 站点ID
     * @param satNo 卫星编号
     * @return S4指数计算结果
     */
    public S4Result calculateS4(String stationId, String satNo) {
        if (stationId == null || stationId.trim().isEmpty()) {
            log.error("站点ID不能为空");
            return null;
        }
        if (satNo == null || satNo.trim().isEmpty()) {
            log.error("卫星编号不能为空");
            return null;
        }

        log.info("开始计算S4指数: stationId={}, satNo={}", stationId, satNo);

        S4Input input = querySnrDataForSingle(stationId, satNo);
        if (input == null || !input.isValid()) {
            log.warn("未获取到有效数据: stationId={}, satNo={}", stationId, satNo);
            return null;
        }

        return calculateSingleS4(input);
    }

    // ==================== 数据库查询方法 ====================

    /**
     * 查询最近1分钟内所有不重复的(站点ID, 卫星编号)组合
     *
     * @return 站点-卫星组合列表
     */
    private List<StationSatellitePair> queryDistinctStationSatellitePairs() {
        long endTime = System.currentTimeMillis();
        long startTime = endTime - ONE_MINUTE_MS;

        String sql = "SELECT DISTINCT station_id, sat_no " +
                "FROM st_sat_obs " +
                "WHERE ts >= ? AND ts <= ? " +
                "AND snr IS NOT NULL ";

        try {
            List<Map<String, Object>> rows = tdengineUtil.queryForList(sql,
                    new java.sql.Timestamp(startTime),
                    new java.sql.Timestamp(endTime));

            if (rows == null || rows.isEmpty()) {
                return Collections.emptyList();
            }

            List<StationSatellitePair> pairs = new ArrayList<>();
            for (Map<String, Object> row : rows) {
                String stationId = row.get("station_id") != null ? row.get("station_id").toString() : null;
                String satNo = row.get("sat_no") != null ? row.get("sat_no").toString() : null;
                if (stationId != null && !stationId.trim().isEmpty()
                        && satNo != null && !satNo.trim().isEmpty()) {
                    pairs.add(new StationSatellitePair(stationId, satNo));
                }
            }
            return pairs;
        } catch (Exception e) {
            log.error("查询站点-卫星组合失败", e);
            return Collections.emptyList();
        }
    }

    /**
     * 批量查询多个组合的SNR数据
     *
     * @param pairs 站点-卫星组合列表
     * @return S4输入参数列表
     */
    private List<S4Input> batchQuerySnrDataForPairs(List<StationSatellitePair> pairs) {
        if (pairs == null || pairs.isEmpty()) {
            return Collections.emptyList();
        }

        long endTime = System.currentTimeMillis();
        long startTime = endTime - ONE_MINUTE_MS;

        List<S4Input> inputs = new ArrayList<>();

        for (StationSatellitePair pair : pairs) {
            S4Input input = querySnrDataForSingleWithTimeRange(
                    pair.stationId, pair.satNo, startTime, endTime);
            if (input != null && input.isValid()) {
                inputs.add(input);
            }
        }

        return inputs;
    }

    /**
     * 查询指定站点和卫星的SNR数据（使用默认时间窗口）
     *
     * @param stationId 站点ID
     * @param satNo 卫星编号
     * @return S4输入参数
     */
    private S4Input querySnrDataForSingle(String stationId, String satNo) {
        long endTime = System.currentTimeMillis();
        long startTime = endTime - ONE_MINUTE_MS;
        return querySnrDataForSingleWithTimeRange(stationId, satNo, startTime, endTime);
    }

    /**
     * 查询指定站点和卫星的SNR数据（指定时间范围）
     *
     * @param stationId 站点ID
     * @param satNo 卫星编号
     * @param startTime 开始时间戳(ms)
     * @param endTime 结束时间戳(ms)
     * @return S4输入参数
     */
    private S4Input querySnrDataForSingleWithTimeRange(String stationId, String satNo,
                                                       long startTime, long endTime) {
        // 使用参数化查询防止SQL注入
        String sql = "SELECT ts, snr FROM st_sat_obs " +
                "WHERE station_id = ? AND sat_no = ? " +
                "AND ts >= ? AND ts <= ? " +
                "AND snr IS NOT NULL " +
                "ORDER BY ts ASC";

        try {
            List<Map<String, Object>> rows = tdengineUtil.queryForList(sql,
                    stationId, satNo,
                    new java.sql.Timestamp(startTime),
                    new java.sql.Timestamp(endTime));

            if (rows == null || rows.isEmpty()) {
                log.debug("未查询到数据: stationId={}, satNo={}", stationId, satNo);
                return null;
            }

            S4Input input = new S4Input();
            List<Double> snrList = new ArrayList<>();
            List<Long> tsList = new ArrayList<>();

            for (Map<String, Object> row : rows) {
                // 获取时间戳
                Object tsObj = row.get("ts");
                if (tsObj instanceof java.sql.Timestamp) {
                    tsList.add(((java.sql.Timestamp) tsObj).getTime());
                } else if (tsObj instanceof Long) {
                    tsList.add((Long) tsObj);
                } else if (tsObj != null) {
                    // 尝试转换
                    try {
                        tsList.add(Long.parseLong(tsObj.toString()));
                    } catch (NumberFormatException e) {
                        log.warn("时间戳格式转换失败: {}", tsObj);
                    }
                }

                // 获取SNR值
                Object snrObj = row.get("snr");
                if (snrObj instanceof Number) {
                    double snr = ((Number) snrObj).doubleValue();
                    // 过滤无效SNR值
                    if (snr > MIN_SNR) {
                        snrList.add(snr);
                    } else {
                        snrList.add(MIN_SNR);
                    }
                } else if (snrObj != null) {
                    try {
                        double snr = Double.parseDouble(snrObj.toString());
                        if (snr > MIN_SNR) {
                            snrList.add(snr);
                        } else {
                            snrList.add(MIN_SNR);
                        }
                    } catch (NumberFormatException e) {
                        log.warn("SNR格式转换失败: {}", snrObj);
                    }
                }
            }

            // 确保时间戳和SNR数量一致
            int minSize = Math.min(snrList.size(), tsList.size());
            if (minSize == 0) {
                log.debug("过滤后无有效数据: stationId={}, satNo={}", stationId, satNo);
                return null;
            }

            input.setSnrSeries(snrList.subList(0, minSize));
            input.setTimestamps(tsList.subList(0, minSize));
            input.setSatNo(satNo);
            input.setStationId(stationId);

            return input;
        } catch (Exception e) {
            log.error("查询SNR数据失败: stationId={}, satNo={}", stationId, satNo, e);
            return null;
        }
    }

    // ==================== S4指数核心计算方法 ====================

    /**
     * 计算单个S4指数
     *
     * @param input 输入参数
     * @return S4计算结果
     */
    private S4Result calculateSingleS4(S4Input input) {
        if (input == null || !input.isValid()) {
            log.warn("输入参数无效");
            return null;
        }

        List<Double> snrSeries = input.getSnrSeries();
        List<Long> timestamps = input.getTimestamps();

        // 1. SNR转信号强度 I = 10^(SNR/10)
        List<Double> intensities = snrSeries.stream()
                .map(snr -> Math.pow(10, snr / 10.0))
                .collect(Collectors.toList());

        // 2. 计算原始强度的均值（用于消趋势）
        double meanRawIntensity = intensities.stream()
                .mapToDouble(Double::doubleValue)
                .average()
                .orElse(1.0);

        // 防止除零
        double lowPassMean = meanRawIntensity > 0 ? meanRawIntensity : 1.0;

        // 3. 消趋势 (Detrending)：I'' = I / <I>
        List<Double> detrendedIntensities = intensities.stream()
                .map(i -> i / lowPassMean)
                .collect(Collectors.toList());

        // 4. 计算消趋势后的均值 <I''>
        double meanDetrended = detrendedIntensities.stream()
                .mapToDouble(Double::doubleValue)
                .average()
                .orElse(1.0);

        // 5. 计算方差 <(I'' - <I''>)^2>
        double variance = detrendedIntensities.stream()
                .mapToDouble(i -> Math.pow(i - meanDetrended, 2))
                .average()
                .orElse(0.0);

        // 6. 计算S4指数 = sqrt(方差) / 均值
        double s4Index = Math.sqrt(Math.max(0, variance)) / Math.max(meanDetrended, 1e-10);

        // 7. 限制S4指数范围 [0, 1]
        s4Index = Math.min(1.0, Math.max(0.0, s4Index));

        // 8. 计算强度标准差（用于输出）
        double intensityStd = Math.sqrt(variance);

        // 9. 构建结果对象
        S4Result result = new S4Result();
        result.setS4Index(s4Index);
        result.setIntensityMean(meanRawIntensity);
        result.setIntensityStd(intensityStd);
        result.setSampleCount(intensities.size());
        result.setSatNo(input.getSatNo());
        result.setStationId(input.getStationId());

        if (timestamps != null && !timestamps.isEmpty()) {
            result.setStartTime(timestamps.get(0));
            result.setEndTime(timestamps.get(timestamps.size() - 1));
        }

        log.debug("S4计算结果: stationId={}, satNo={}, s4Index={}, sampleCount={}",
                result.getStationId(), result.getSatNo(),
                String.format("%.4f", result.getS4Index()), result.getSampleCount());

        return result;
    }

    // ==================== 内部辅助类 ====================

    /**
     * 站点-卫星组合内部类
     */
    private static class StationSatellitePair {
        String stationId;
        String satNo;

        StationSatellitePair(String stationId, String satNo) {
            this.stationId = stationId;
            this.satNo = satNo;
        }
    }
}