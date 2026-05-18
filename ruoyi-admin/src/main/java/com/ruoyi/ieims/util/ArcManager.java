package com.ruoyi.ieims.util;

import com.ruoyi.ieims.gnss.domain.GnssStation;
import com.ruoyi.ieims.gnss.domain.TecCalculationArc;
import com.ruoyi.ieims.gnss.domain.TecCycleSlipResult;
import com.ruoyi.ieims.gnss.domain.TecSatObs;
import com.ruoyi.ieims.gnss.service.IGnssStationService;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 实时弧段管理器（滑动窗口 + TTL清理）
 * 坐标来源：gnss_station 配置表（IGnssStationService）
 *
 * @author guet_developer01
 * @date 2026-05-12
 */
@Slf4j
@Component
public class ArcManager {

    private final ConcurrentHashMap<String, TecCalculationArc> activeArcs = new ConcurrentHashMap<>();

    @Autowired
    private CycleSlipDetector slipDetector;

    @Autowired
    private IGnssStationService gnssStationService; // 【新增】你的站点服务

    public void ingestObservation(TecSatObs obs) {
        String key = obs.getStationId() + ":" + obs.getSatNo();
        TecCalculationArc arc = activeArcs.computeIfAbsent(key, k -> {
            TecCalculationArc a = new TecCalculationArc();
            a.setStationId(obs.getStationId());
            a.setSatNo(obs.getSatNo());
            // 【关键】创建弧段时从数据库加载站点坐标
            loadStationCoordinate(a, obs.getStationId());
            return a;
        });

        // 数据中断检测：超过阈值重置弧段
        if (arc.getLastTimestamp() != null
                && (obs.getTs().getTime() - arc.getLastTimestamp()) > TecConstant.MAX_GAP_SECONDS * 1000) {
            arc.reset();
            loadStationCoordinate(arc, obs.getStationId()); // 重置后重新加载坐标（可能已更新）
        }

        TecCycleSlipResult slip = slipDetector.detect(obs, arc);
        if (slip.isSlipped()) {
            arc.reset();
            arc.incrementSlipCount();
            loadStationCoordinate(arc, obs.getStationId()); // 周跳后重新加载
        }

        arc.addObservation(obs);
        arc.evictOlderThan(TecConstant.MAX_ARC_MINUTES);
        arc.setLastTimestamp(obs.getTs().getTime());
    }

    /**
     * 从 gnss_station 表查询站点坐标并注入弧段
     * 【注意】请确认 GnssStation 实体中 lat/lon 的 getter 名称，若不同请修改
     */
    private void loadStationCoordinate(TecCalculationArc arc, String stationId) {
        try {
            GnssStation station = gnssStationService.selectGnssStationByStationId(stationId);
            if (station != null) {
                // 根据你的 GnssStation 实体实际字段调整，常见为 getLatitude() / getLongitude()
                Double lat = station.getLatitude();
                Double lon = station.getLongitude();

                if (lat != null && lon != null) {
                    arc.setStationLat(lat);
                    arc.setStationLon(lon);
                    log.debug("弧段注入坐标: station={}, lat={:.6f}, lon={:.6f}", stationId, lat, lon);
                } else {
                    log.warn("站点坐标字段为空: station={}, lat={}, lon={}", stationId, lat, lon);
                }
            } else {
                log.warn("未找到站点配置: station={}, IPP将无法计算", stationId);
            }
        } catch (Exception e) {
            log.error("查询站点坐标异常: station={}", stationId, e);
        }
    }

    /**
     * 获取满足计算条件的弧段（返回 Map 便于计算后精准移除）
     */
    public Map<String, TecCalculationArc> getReadyArcsForCalculation() {
        Map<String, TecCalculationArc> ready = new HashMap<>();
        long now = System.currentTimeMillis();

        for (Map.Entry<String, TecCalculationArc> entry : activeArcs.entrySet()) {
            TecCalculationArc arc = entry.getValue();
            // 清理过期弧段
            if (arc.getLastTimestamp() != null
                    && (now - arc.getLastTimestamp()) > TecConstant.ARC_TTL_MINUTES * 60 * 1000) {
                activeArcs.remove(entry.getKey());
                continue;
            }
            if (arc.size() >= TecConstant.MIN_VALID_EPOCHS && arc.hasDataInLastMinutes(10)) {
                ready.put(entry.getKey(), arc);
            }
        }
        return ready;
    }

    public void removeArc(String key) {
        activeArcs.remove(key);
    }
}