package com.ruoyi.ieims.util;

import com.ruoyi.ieims.gnss.domain.TecCalculationArc;
import com.ruoyi.ieims.gnss.domain.TecResult;
import com.ruoyi.ieims.gnss.domain.TecSatObs;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import java.util.Date;
import java.util.List;

@Component
public class TecCalculator {

    @Autowired
    private DcbManager dcbManager;

    /**
     * 计算单弧段 TEC
     * @param arc 观测弧段
     * @param key ArcManager 中的 key（stationId:satNo），仅用于日志
     */
    public TecResult calculate(TecCalculationArc arc, String key) {
        List<TecSatObs> obsList = arc.getObservations();
        if (obsList == null || obsList.size() < TecConstant.MIN_VALID_EPOCHS) {
            return null;
        }

        String satNo = arc.getSatNo();
        String stationId = arc.getStationId();
        double f1 = TecFrequencyUtil.getFrequency(satNo, 1);
        double f2 = TecFrequencyUtil.getFrequency(satNo, 2);
        double lambda1 = TecConstant.SPEED_OF_LIGHT / f1;
        double lambda2 = TecConstant.SPEED_OF_LIGHT / f2;

        Date arcDate = obsList.get(0).getTs();
        double satDcb = dcbManager.getSatelliteDcb(satNo, arcDate);
        double rcvDcb = dcbManager.getReceiverDcb(stationId);
        double totalDcb = satDcb + rcvDcb;
        boolean dcbCorrected = (totalDcb != 0.0);

        double sumNumerator = 0.0;
        double sumDenominator = 0.0;
        double minElev = Double.MAX_VALUE;
        double maxElev = -Double.MAX_VALUE;
        double lastTecA = Double.NaN;

        TecSatObs lastObs = null;
        int validCount = 0;

        for (TecSatObs obs : obsList) {
            Double elevObj = obs.getElevation();
            if (elevObj == null || elevObj < TecConstant.MIN_ELEVATION) continue;

            Double p1Obj = obs.getPseudorangeP1();
            Double p2Obj = obs.getPseudorangeP2();
            Double l1Obj = obs.getPhaseL1();
            Double l2Obj = obs.getPhaseP2();
            if (p1Obj == null || p2Obj == null || l1Obj == null || l2Obj == null) continue;

            double tecA = calculateTECA(p1Obj, p2Obj, f1, f2) - totalDcb;
            double tecR = calculateTECR(l1Obj * lambda1, l2Obj * lambda2, f1, f2);

            if (!Double.isNaN(lastTecA)) {
                if (Math.abs(tecA - lastTecA) > TecConstant.PSEUDORANGE_JUMP_THRESHOLD) continue;
            }

            minElev = Math.min(minElev, elevObj);
            maxElev = Math.max(maxElev, elevObj);

            double weight = Math.pow(Math.sin(Math.toRadians(elevObj)), 2);
            sumNumerator += (tecA - tecR) * weight;
            sumDenominator += weight;
            validCount++;
            lastObs = obs;
            lastTecA = tecA;
        }

        if (validCount < TecConstant.MIN_VALID_EPOCHS || sumDenominator == 0.0 || lastObs == null) {
            return null;
        }

        double elevSpan = maxElev - minElev;
        if (elevSpan > TecConstant.MAX_ELEV_SPAN) {
            return null;
        }

        // 质量标志
        String qualityFlag;
        if (dcbCorrected && elevSpan > 10.0) qualityFlag = TecConstant.QUALITY_GOOD;
        else if (dcbCorrected) qualityFlag = TecConstant.QUALITY_SUSPECT;
        else qualityFlag = TecConstant.QUALITY_RELATIVE;

        double bR = sumNumerator / sumDenominator;
        double finalL1 = lastObs.getPhaseL1() * lambda1;
        double finalL2 = lastObs.getPhaseP2() * lambda2;
        double finalTecR = calculateTECR(finalL1, finalL2, f1, f2);
        double stec = finalTecR + bR;

        double lastElev = lastObs.getElevation() != null ? lastObs.getElevation() : 30.0;
        double mapping = TecMappingFunctionUtil.calculateSlm(lastElev);
        double vtec = stec / mapping;

        TecResult r = new TecResult();
        r.setStationId(stationId);
        r.setSatNo(satNo);
        r.setTs(lastObs.getTs());
        r.setStec(stec);
        r.setVtec(vtec);
        r.setDcbEstimate(bR);
        r.setValidEpochCount(validCount);
        r.setElevSpan(elevSpan);
        r.setQualityFlag(qualityFlag);
        r.setDcbCorrected(dcbCorrected);
        r.setSlipCount(arc.getSlipCount());
        r.setMappingFunc(TecConstant.MAPPING_SLM);
        r.setSatSystem(lastObs.getSatSystem() != null ? lastObs.getSatSystem() : String.valueOf(satNo.charAt(0)));

        // 【新增】IPP 计算
        Double stationLat = arc.getStationLat();
        Double stationLon = arc.getStationLon();
        Double azimuth = lastObs.getAzimuth();
        if (stationLat != null && stationLon != null && azimuth != null) {
            double[] ipp = TecMappingFunctionUtil.calculateIpp(stationLat, stationLon, lastElev, azimuth);
            r.setIppLat(ipp[0]);
            r.setIppLon(ipp[1]);
        } else {
            r.setIppLat(null);
            r.setIppLon(null);
            log.warn("IPP计算条件不足，缺失坐标或方位角: key={}", key);
        }

        return r;
    }

    private double calculateTECA(double p1, double p2, double f1, double f2) {
        double factor = (f1 * f1 * f2 * f2) / (TecConstant.COEFFICIENT * (f1 * f1 - f2 * f2));
        return factor * (p2 - p1) / TecConstant.TEC_UNIT;
    }

    private double calculateTECR(double l1M, double l2M, double f1, double f2) {
        double factor = (f1 * f1 * f2 * f2) / (TecConstant.COEFFICIENT * (f1 * f1 - f2 * f2));
        return factor * (l1M - l2M) / TecConstant.TEC_UNIT;
    }

    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(TecCalculator.class);
}