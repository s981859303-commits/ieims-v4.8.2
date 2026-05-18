package com.ruoyi.ieims.gnss.domain;

/**
 * 周跳探测结果
 *
 * @author guet_developer01
 * @date 2026-05-12
 */
public class TecCycleSlipResult {

    private boolean slipped;
    private String reason;

    public static TecCycleSlipResult noSlip() {
        TecCycleSlipResult r = new TecCycleSlipResult();
        r.slipped = false;
        return r;
    }

    public static TecCycleSlipResult slip(String reason) {
        TecCycleSlipResult r = new TecCycleSlipResult();
        r.slipped = true;
        r.reason = reason;
        return r;
    }

    public boolean isSlipped() {
        return slipped;
    }

    public String getReason() {
        return reason;
    }
}