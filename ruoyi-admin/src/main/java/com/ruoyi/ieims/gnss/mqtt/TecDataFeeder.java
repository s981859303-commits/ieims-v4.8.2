package com.ruoyi.ieims.gnss.mqtt;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ruoyi.ieims.gnss.domain.TecSatObs;
import com.ruoyi.ieims.util.ArcManager;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;

@Slf4j
@Component
public class TecDataFeeder {

    @Autowired
    private ArcManager arcManager;

    public void onObservationMessage(String topic, String payload) {
        try {
            if (payload == null || payload.trim().isEmpty()) return;
            String trimmed = payload.trim();
            if (trimmed.startsWith("[")) {
                JSONArray arr = JSON.parseArray(trimmed);
                if (arr == null || arr.isEmpty()) return;
                for (int i = 0; i < arr.size(); i++) processSingleObservation(arr.getJSONObject(i));
            } else if (trimmed.startsWith("{")) {
                processSingleObservation(JSON.parseObject(trimmed));
            } else {
                log.warn("TEC数据格式未知: {}", trimmed.substring(0, Math.min(100, trimmed.length())));
            }
        } catch (Exception e) {
            log.error("TEC观测数据解析失败, topic={}, payload前200字符={}", topic,
                    payload.substring(0, Math.min(200, payload.length())), e);
        }
    }

    private void processSingleObservation(JSONObject json) {
        if (json == null) return;
        try {
            TecSatObs obs = new TecSatObs();
            obs.setStationId(getString(json, "stationId"));
            obs.setSatNo(getString(json, "satNo"));
            obs.setSatSystem(getString(json, "satSystem"));

            Long epochTime = getLong(json, "epochTime");
            Long timestamp = getLong(json, "timestamp");
            if (epochTime != null && epochTime > 0) obs.setTs(new Date(epochTime));
            else if (timestamp != null && timestamp > 0) obs.setTs(new Date(timestamp));
            else obs.setTs(new Date());

            // 高度角、方位角允许 0.0
            obs.setElevation(getDoubleAllowZero(json, "elevation"));
            obs.setAzimuth(getDoubleAllowZero(json, "azimuth"));

            // 伪距：0.0 视为无效（设备未输出该频点）
            obs.setPseudorangeP1(getDoubleNonZero(json, "pseudorangeP1"));
            obs.setPseudorangeP2(getDoubleNonZero(json, "pseudorangeP2"));

            // 相位：0.0 可能合法（整周模糊度附近或单频场景）
            obs.setPhaseL1(getDoubleAllowZero(json, "phaseL1"));
            obs.setPhaseP2(getDoubleAllowZero(json, "phaseP2"));

            // 完整性校验：伪距双频+相位双频+高度角必须齐全
            if (obs.getPseudorangeP1() == null || obs.getPseudorangeP2() == null
                    || obs.getPhaseL1() == null || obs.getPhaseP2() == null
                    || obs.getElevation() == null) {
                log.debug("TEC数据不完整，跳过: station={}, sat={}, p1={}, p2={}, l1={}, l2={}, elev={}",
                        obs.getStationId(), obs.getSatNo(),
                        obs.getPseudorangeP1(), obs.getPseudorangeP2(),
                        obs.getPhaseL1(), obs.getPhaseP2(), obs.getElevation());
                return;
            }

            arcManager.ingestObservation(obs);

        } catch (Exception e) {
            log.error("TEC单条观测处理失败, json={}", json.toJSONString(), e);
        }
    }

    private String getString(JSONObject json, String key) {
        if (json == null || !json.containsKey(key)) return null;
        Object val = json.get(key);
        return val != null ? val.toString() : null;
    }

    private Long getLong(JSONObject json, String key) {
        if (json == null || !json.containsKey(key)) return null;
        Object val = json.get(key);
        if (val == null) return null;
        if (val instanceof Number) return ((Number) val).longValue();
        try { return Long.parseLong(val.toString()); } catch (NumberFormatException e) { return null; }
    }

    /**
     * 获取 Double，0.0 视为无效（用于伪距）
     */
    private Double getDoubleNonZero(JSONObject json, String key) {
        Double val = getDoubleRaw(json, key);
        return (val != null && val != 0.0) ? val : null;
    }

    /**
     * 获取 Double，保留 0.0（用于相位、高度角、方位角）
     */
    private Double getDoubleAllowZero(JSONObject json, String key) {
        return getDoubleRaw(json, key);
    }

    /**
     * 原始解析，不过滤 0.0
     */
    private Double getDoubleRaw(JSONObject json, String key) {
        if (json == null || !json.containsKey(key)) return null;
        Object val = json.get(key);
        if (val == null) return null;
        if (val instanceof Number) return ((Number) val).doubleValue();
        try { return Double.parseDouble(val.toString()); } catch (NumberFormatException e) { return null; }
    }
}