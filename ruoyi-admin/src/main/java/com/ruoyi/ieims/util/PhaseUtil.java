package com.ruoyi.ieims.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * 相位闪烁指数计算工具类 (MAD鲁棒估算版)
 *
 * @author guet_developer01
 * @date 2026-05-11
 */
@Component
public class PhaseUtil {

    private static final Logger log = LoggerFactory.getLogger(PhaseUtil.class);

    public enum ScintillationLevel {
        NONE("无闪烁", 0, "信号稳定，无明显相位扰动", 0.0, 0.2),
        WEAK("弱闪烁", 1, "存在轻微相位抖动，可能影响精密定位", 0.2, 0.4),
        MODERATE("中等闪烁", 2, "明显相位抖动，定位精度下降", 0.4, 0.6),
        SEVERE("强闪烁", 3, "严重相位扰动，可能导致信号失锁", 0.6, 100.0);

        private final String chineseName;
        private final int code;
        private final String description;
        private final double minPhi4;
        private final double maxPhi4;

        ScintillationLevel(String chineseName, int code, String description,
                           double minPhi4, double maxPhi4) {
            this.chineseName = chineseName;
            this.code = code;
            this.description = description;
            this.minPhi4 = minPhi4;
            this.maxPhi4 = maxPhi4;
        }

        public String getChineseName() { return chineseName; }
        public int getCode() { return code; }
        public String getDescription() { return description; }
        public double getMinPhi4() { return minPhi4; }
        public double getMaxPhi4() { return maxPhi4; }

        public static ScintillationLevel fromPhi4(double phi4) {
            if (phi4 < 0.2) return NONE;
            if (phi4 < 0.4) return WEAK;
            if (phi4 < 0.6) return MODERATE;
            return SEVERE;
        }

        public String getColor() {
            switch (this) {
                case NONE: return "#00C851";
                case WEAK: return "#FFBB33";
                case MODERATE: return "#FF8800";
                case SEVERE: return "#FF4444";
                default: return "#AAAAAA";
            }
        }
    }

    public static class PhaseInput {
        private List<Double> phaseSeries;
        private List<Long> timestamps;
        private String satNo;
        private String stationId;
        private Long startTime;
        private Long endTime;

        public List<Double> getPhaseSeries() { return phaseSeries; }
        public void setPhaseSeries(List<Double> phaseSeries) { this.phaseSeries = phaseSeries; }
        public List<Long> getTimestamps() { return timestamps; }
        public void setTimestamps(List<Long> timestamps) { this.timestamps = timestamps; }
        public String getSatNo() { return satNo; }
        public void setSatNo(String satNo) { this.satNo = satNo; }
        public String getStationId() { return stationId; }
        public void setStationId(String stationId) { this.stationId = stationId; }
        public Long getStartTime() { return startTime; }
        public void setStartTime(Long startTime) { this.startTime = startTime; }
        public Long getEndTime() { return endTime; }
        public void setEndTime(Long endTime) { this.endTime = endTime; }
    }

    public static class PhaseResult {
        private double phi4;
        private double rawSigmaPhi;
        private Long startTime;
        private Long endTime;
        private int sampleCount;
        private String satNo;
        private String stationId;
        private Long calcTime;

        public PhaseResult() { this.calcTime = System.currentTimeMillis(); }
        public double getPhi4() { return phi4; }
        public void setPhi4(double phi4) { this.phi4 = phi4; }
        public double getRawSigmaPhi() { return rawSigmaPhi; }
        public void setRawSigmaPhi(double rawSigmaPhi) { this.rawSigmaPhi = rawSigmaPhi; }
        public Long getStartTime() { return startTime; }
        public void setStartTime(Long startTime) { this.startTime = startTime; }
        public Long getEndTime() { return endTime; }
        public void setEndTime(Long endTime) { this.endTime = endTime; }
        public int getSampleCount() { return sampleCount; }
        public void setSampleCount(int sampleCount) { this.sampleCount = sampleCount; }
        public String getSatNo() { return satNo; }
        public void setSatNo(String satNo) { this.satNo = satNo; }
        public String getStationId() { return stationId; }
        public void setStationId(String stationId) { this.stationId = stationId; }
        public Long getCalcTime() { return calcTime; }
        public void setCalcTime(Long calcTime) { this.calcTime = calcTime; }
    }

    public static class PhaseResultWithLevel extends PhaseResult {
        private ScintillationLevel level;
        private String levelName;
        private String levelColor;
        private String alertMessage;

        public PhaseResultWithLevel(PhaseResult base) {
            this.setPhi4(base.getPhi4());
            this.setRawSigmaPhi(base.getRawSigmaPhi());
            this.setStartTime(base.getStartTime());
            this.setEndTime(base.getEndTime());
            this.setSampleCount(base.getSampleCount());
            this.setSatNo(base.getSatNo());
            this.setStationId(base.getStationId());
            this.setCalcTime(base.getCalcTime());
            this.level = ScintillationLevel.fromPhi4(base.getPhi4());
            this.levelName = this.level.getChineseName();
            this.levelColor = this.level.getColor();
            this.alertMessage = generateAlertMessage();
        }

        private String generateAlertMessage() {
            double phi4 = this.getPhi4();
            switch (level) {
                case NONE: return "正常";
                case WEAK: return String.format("弱闪烁 (φ₄=%.3f)，建议关注", phi4);
                case MODERATE: return String.format("中等闪烁 (φ₄=%.3f)，定位精度可能受影响", phi4);
                case SEVERE: return String.format("强闪烁 (φ₄=%.3f)，可能出现信号失锁，请及时处理", phi4);
                default: return "未知状态";
            }
        }
        public ScintillationLevel getLevel() { return level; }
        public void setLevel(ScintillationLevel level) { this.level = level; }
        public String getLevelName() { return levelName; }
        public void setLevelName(String levelName) { this.levelName = levelName; }
        public String getLevelColor() { return levelColor; }
        public void setLevelColor(String levelColor) { this.levelColor = levelColor; }
        public String getAlertMessage() { return alertMessage; }
        public void setAlertMessage(String alertMessage) { this.alertMessage = alertMessage; }
    }

    public static class ScintillationSummary {
        private Long reportTime;
        private String overallStatus;
        private String overallColor;
        private Integer totalSatellites;
        private Map<String, Integer> levelDistribution;
        private List<StationScintillationStat> stationStats;
        private List<PhaseResultWithLevel> details;

        public ScintillationSummary() {
            this.reportTime = System.currentTimeMillis();
            this.levelDistribution = new HashMap<>();
            this.stationStats = new ArrayList<>();
            this.details = new ArrayList<>();
        }

        public Long getReportTime() { return reportTime; }
        public void setReportTime(Long reportTime) { this.reportTime = reportTime; }
        public String getOverallStatus() { return overallStatus; }
        public void setOverallStatus(String overallStatus) { this.overallStatus = overallStatus; }
        public String getOverallColor() { return overallColor; }
        public void setOverallColor(String overallColor) { this.overallColor = overallColor; }
        public Integer getTotalSatellites() { return totalSatellites; }
        public void setTotalSatellites(Integer totalSatellites) { this.totalSatellites = totalSatellites; }
        public Map<String, Integer> getLevelDistribution() { return levelDistribution; }
        public void setLevelDistribution(Map<String, Integer> levelDistribution) { this.levelDistribution = levelDistribution; }
        public List<StationScintillationStat> getStationStats() { return stationStats; }
        public void setStationStats(List<StationScintillationStat> stationStats) { this.stationStats = stationStats; }
        public List<PhaseResultWithLevel> getDetails() { return details; }
        public void setDetails(List<PhaseResultWithLevel> details) { this.details = details; }
    }

    public static class StationScintillationStat {
        private String stationId;
        private Integer satelliteCount;
        private Double maxPhi4;
        private Double avgPhi4;
        private String worstLevel;
        private String worstLevelColor;
        private Map<String, Integer> levelDistribution;

        public StationScintillationStat() { this.levelDistribution = new HashMap<>(); }
        public String getStationId() { return stationId; }
        public void setStationId(String stationId) { this.stationId = stationId; }
        public Integer getSatelliteCount() { return satelliteCount; }
        public void setSatelliteCount(Integer satelliteCount) { this.satelliteCount = satelliteCount; }
        public Double getMaxPhi4() { return maxPhi4; }
        public void setMaxPhi4(Double maxPhi4) { this.maxPhi4 = maxPhi4; }
        public Double getAvgPhi4() { return avgPhi4; }
        public void setAvgPhi4(Double avgPhi4) { this.avgPhi4 = avgPhi4; }
        public String getWorstLevel() { return worstLevel; }
        public void setWorstLevel(String worstLevel) { this.worstLevel = worstLevel; }
        public String getWorstLevelColor() { return worstLevelColor; }
        public void setWorstLevelColor(String worstLevelColor) { this.worstLevelColor = worstLevelColor; }
        public Map<String, Integer> getLevelDistribution() { return levelDistribution; }
        public void setLevelDistribution(Map<String, Integer> levelDistribution) { this.levelDistribution = levelDistribution; }
    }

    /**
     * ✨ 核心升级：基于二次差分与MAD的中位数绝对偏差鲁棒估算
     * 完美免疫接收机钟跳、数据断层与极值毛刺
     */
    public double calculateSigmaPhi(List<Double> phaseSeries, List<Long> timestamps) {
        if (phaseSeries == null || timestamps == null || phaseSeries.size() < 10) {
            return -1.0;
        }

        // 1. 转为弧度
        List<Double> radSeries = phaseSeries.stream()
                .map(phi -> phi * 2.0 * Math.PI)
                .collect(Collectors.toList());

        // 2. 二次差分隔离高频抖动 (忽略宏观运动)
        List<Double> secondDiffs = new ArrayList<>();
        for (int i = 2; i < radSeries.size(); i++) {
            double dt1 = Math.abs(timestamps.get(i - 1) - timestamps.get(i - 2)) / 1000.0;
            double dt2 = Math.abs(timestamps.get(i) - timestamps.get(i - 1)) / 1000.0;

            // 过滤掉断点数据，只针对约 1 秒连续的数据做差分
            if (dt1 > 0.5 && dt1 < 2.0 && dt2 > 0.5 && dt2 < 2.0) {
                double p0 = radSeries.get(i - 2);
                double p1 = radSeries.get(i - 1);
                double p2 = radSeries.get(i);

                // 二次差分 d2 = x2 - 2x1 + x0
                secondDiffs.add(p2 - 2.0 * p1 + p0);
            }
        }

        if (secondDiffs.size() < 5) return -1.0;

        // 3. 计算 MAD (中位数绝对偏差)，这一步让算法对钟跳极值完全免疫
        Collections.sort(secondDiffs);
        double median = getMedian(secondDiffs);

        List<Double> absDevs = new ArrayList<>(secondDiffs.size());
        for (Double val : secondDiffs) {
            absDevs.add(Math.abs(val - median));
        }
        Collections.sort(absDevs);
        double mad = getMedian(absDevs);

        // 4. 数学反推标准差：std ≈ 1.4826 * MAD， 二次差分放大了噪声，除以 sqrt(6)
        double stdD2 = 1.4826 * mad;
        return stdD2 / Math.sqrt(6.0);
    }

    private double getMedian(List<Double> sortedData) {
        int n = sortedData.size();
        if (n == 0) return 0.0;
        if (n % 2 == 0) {
            return (sortedData.get(n / 2 - 1) + sortedData.get(n / 2)) / 2.0;
        } else {
            return sortedData.get(n / 2);
        }
    }

    public double mapToPhi4(double sigmaPhi) {
        if (sigmaPhi < 0) return -1.0;
        // 如果实在因为其他干扰大于 5.0 的，直接封顶展示
        return Math.min(sigmaPhi, 5.0);
    }

    public PhaseResult calculatePhase(PhaseInput input) {
        PhaseResult result = new PhaseResult();
        if (input == null || input.getPhaseSeries() == null || input.getTimestamps() == null) {
            result.setPhi4(-1.0); result.setRawSigmaPhi(-1.0); return result;
        }

        result.setSatNo(input.getSatNo());
        result.setStationId(input.getStationId());
        result.setStartTime(input.getStartTime());
        result.setEndTime(input.getEndTime());
        result.setSampleCount(input.getPhaseSeries().size());

        if (input.getPhaseSeries().size() < 10) {
            result.setPhi4(-1.0); result.setRawSigmaPhi(-1.0); return result;
        }

        double rawSigmaPhi = calculateSigmaPhi(input.getPhaseSeries(), input.getTimestamps());
        result.setRawSigmaPhi(rawSigmaPhi);
        result.setPhi4(mapToPhi4(rawSigmaPhi));

        return result;
    }

    public PhaseResultWithLevel calculatePhaseWithLevel(PhaseInput input) {
        return new PhaseResultWithLevel(calculatePhase(input));
    }

    public List<PhaseResultWithLevel> batchCalculate(List<PhaseInput> inputs) {
        if (inputs == null || inputs.isEmpty()) return new ArrayList<>();
        return inputs.stream()
                .map(this::calculatePhaseWithLevel)
                .filter(r -> r.getPhi4() >= 0)
                .collect(Collectors.toList());
    }

    public ScintillationSummary buildSummary(List<PhaseResultWithLevel> results) {
        ScintillationSummary summary = new ScintillationSummary();

        if (results == null || results.isEmpty()) {
            summary.setOverallStatus("无数据");
            summary.setOverallColor("#AAAAAA");
            summary.setTotalSatellites(0);
            return summary;
        }

        summary.setTotalSatellites(results.size());
        summary.setDetails(results);

        // 必须按 StationId 分组才能正确算出总卫星数！
        Map<String, List<PhaseResultWithLevel>> stationGroupMap = results.stream()
                .collect(Collectors.groupingBy(PhaseResultWithLevel::getStationId));

        List<StationScintillationStat> stationStats = new ArrayList<>();
        int severeCount = 0, moderateCount = 0, weakCount = 0, noneCount = 0;

        for (Map.Entry<String, List<PhaseResultWithLevel>> entry : stationGroupMap.entrySet()) {
            String stationId = entry.getKey();
            List<PhaseResultWithLevel> stationResults = entry.getValue();

            StationScintillationStat stat = new StationScintillationStat();
            stat.setStationId(stationId);
            stat.setSatelliteCount(stationResults.size());

            double maxPhi4 = 0, sumPhi4 = 0;
            Map<String, Integer> stationLevelDist = new HashMap<>();

            for (PhaseResultWithLevel r : stationResults) {
                double phi4 = r.getPhi4();
                maxPhi4 = Math.max(maxPhi4, phi4);
                sumPhi4 += phi4;

                String levelName = r.getLevelName();
                stationLevelDist.put(levelName, stationLevelDist.getOrDefault(levelName, 0) + 1);
            }

            stat.setMaxPhi4(maxPhi4);
            stat.setAvgPhi4(sumPhi4 / stationResults.size());
            stat.setLevelDistribution(stationLevelDist);

            if (stationLevelDist.containsKey("强闪烁")) { stat.setWorstLevel("强闪烁"); stat.setWorstLevelColor("#FF4444"); }
            else if (stationLevelDist.containsKey("中等闪烁")) { stat.setWorstLevel("中等闪烁"); stat.setWorstLevelColor("#FF8800"); }
            else if (stationLevelDist.containsKey("弱闪烁")) { stat.setWorstLevel("弱闪烁"); stat.setWorstLevelColor("#FFBB33"); }
            else { stat.setWorstLevel("无闪烁"); stat.setWorstLevelColor("#00C851"); }

            severeCount += stationLevelDist.getOrDefault("强闪烁", 0);
            moderateCount += stationLevelDist.getOrDefault("中等闪烁", 0);
            weakCount += stationLevelDist.getOrDefault("弱闪烁", 0);
            noneCount += stationLevelDist.getOrDefault("无闪烁", 0);

            stationStats.add(stat);
        }

        summary.setStationStats(stationStats);

        Map<String, Integer> globalLevelDist = new HashMap<>();
        globalLevelDist.put("无闪烁", noneCount);
        globalLevelDist.put("弱闪烁", weakCount);
        globalLevelDist.put("中等闪烁", moderateCount);
        globalLevelDist.put("强闪烁", severeCount);
        summary.setLevelDistribution(globalLevelDist);

        if (severeCount > 0) { summary.setOverallStatus("⚠️ 告警：存在强闪烁事件"); summary.setOverallColor("#FF4444"); }
        else if (moderateCount > 0) { summary.setOverallStatus("⚡ 注意：存在中等闪烁事件"); summary.setOverallColor("#FF8800"); }
        else if (weakCount > 0) { summary.setOverallStatus("ℹ️ 提示：存在弱闪烁事件"); summary.setOverallColor("#FFBB33"); }
        else { summary.setOverallStatus("✅ 所有信号正常"); summary.setOverallColor("#00C851"); }

        return summary;
    }
}