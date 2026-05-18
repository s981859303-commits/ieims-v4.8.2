package com.ruoyi.ieims.util;

import com.ruoyi.ieims.gnss.domain.TecCalculationArc;
import com.ruoyi.ieims.gnss.domain.TecCycleSlipResult;
import com.ruoyi.ieims.gnss.domain.TecSatObs;
import org.springframework.stereotype.Component;

/**
 * Melbourne-Wübbena + GF 组合周跳探测器
 *
 * @author guet_developer01
 * @date 2026-05-12
 */
@Component
public class CycleSlipDetector {

    public TecCycleSlipResult detect(TecSatObs obs, TecCalculationArc arc) {
        double f1 = TecFrequencyUtil.getFrequency(obs.getSatNo(), 1);
        double f2 = TecFrequencyUtil.getFrequency(obs.getSatNo(), 2);
        double c = TecConstant.SPEED_OF_LIGHT;

        if (obs.getPseudorangeP1() == null || obs.getPseudorangeP2() == null
                || obs.getPhaseL1() == null || obs.getPhaseP2() == null
                || obs.getElevation() == null) {
            return TecCycleSlipResult.noSlip();
        }

        double lambdaW = c / Math.abs(f1 - f2);
        double term1 = obs.getPhaseL1() - obs.getPhaseP2();
        double term2 = (f1 * obs.getPseudorangeP1() + f2 * obs.getPseudorangeP2())
                / (f1 + f2) / lambdaW;
        double mw = term1 - term2;

        double lambda1 = c / f1;
        double lambda2 = c / f2;
        double gf = obs.getPhaseL1() * lambda1 - obs.getPhaseP2() * lambda2;

        if (arc.getLastMw() == null) {
            arc.setLastMw(mw);
            arc.setLastGf(gf);
            arc.setMwMean(mw);
            return TecCycleSlipResult.noSlip();
        }

        double alpha = 0.05;
        double mwMean = arc.getMwMean() == null ? mw : arc.getMwMean();
        mwMean = alpha * mw + (1 - alpha) * mwMean;
        arc.setMwMean(mwMean);

        double diffMw = Math.abs(mw - arc.getLastMw());
        double diffGf = Math.abs(gf - arc.getLastGf());

        long timeGap = 30;
        if (arc.getLastTimestamp() != null && obs.getTs() != null) {
            timeGap = (obs.getTs().getTime() - arc.getLastTimestamp()) / 1000L;
        }
        double adaptiveGfThreshold = TecConstant.GF_SLIP_THRESHOLD_METERS
                + Math.max(timeGap - 30.0, 0) * 0.0015;

        boolean mwSlip = diffMw > TecConstant.MW_SLIP_THRESHOLD_CYCLES;
        boolean gfSlip = diffGf > adaptiveGfThreshold;

        if (mwSlip || gfSlip) {
            return TecCycleSlipResult.slip(String.format("MW:%.2f|GF:%.3f", diffMw, diffGf));
        }

        arc.setLastMw(mw);
        arc.setLastGf(gf);
        return TecCycleSlipResult.noSlip();
    }
}