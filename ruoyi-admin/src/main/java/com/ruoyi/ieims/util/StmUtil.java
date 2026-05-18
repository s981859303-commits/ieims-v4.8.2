package com.ruoyi.ieims.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ruoyi.user.comm.core.websocket.WebSocketMsg;
import com.ruoyi.user.comm.core.websocket.WebSocketUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Component
public class StmUtil {

    @Autowired
    private WebSocketUtil webSocketUtil;

    /**
     * 解析并转发 MQTT 批量观测数据到天空图大屏
     * 注意：方法名已改为 processMqttPayload 以匹配你的调用
     */
    public void processMqttPayload(String payload) {
        long startTime = System.currentTimeMillis();
        try {
            JSONArray jsonArray = JSON.parseArray(payload);
            if (jsonArray == null || jsonArray.isEmpty()) return;

            List<SatObsDTO> detailList = new ArrayList<>();
            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject obs = jsonArray.getJSONObject(i);
                SatObsDTO dto = new SatObsDTO();
                dto.setSatNo(obs.getString("satNo"));
                String sys = obs.getString("satSystem");
                dto.setSatSystem(sys);
                dto.setCountry(getCountryBySystem(sys));
                dto.setAzimuth(obs.getDoubleValue("azimuth"));
                dto.setElevation(obs.getDoubleValue("elevation"));
                dto.setStationId(obs.getString("stationId"));
                detailList.add(dto);
            }

            // 计算天空图所需的实时统计数据
            Map<String, Object> stats = new HashMap<>();
            stats.put("totalSatellites", detailList.size());
            stats.put("visibleSatellites", detailList.stream().filter(s -> s.getElevation() > 5.0).count());
            stats.put("averageElevation", Math.round(detailList.stream().mapToDouble(SatObsDTO::getElevation).average().orElse(0.0) * 10) / 10.0);
            stats.put("updateTime", new Date());

            // 国家分类统计
            Map<String, Long> countryCount = detailList.stream()
                    .collect(Collectors.groupingBy(SatObsDTO::getCountry, Collectors.counting()));
            stats.put("countryCount", countryCount);
            stats.put("detailList", detailList);

            // 推送到天空图 WebSocket 主题
            WebSocketMsg msg = new WebSocketMsg()
                    .setMsgType("data")
                    .setExtraData(JSON.toJSONString(stats))
                    .setSendTime(LocalDateTime.now());
            webSocketUtil.sendToTopic("/topic/constellation", msg);

            log.info("天空图实时推送完成，卫星数: {}，耗时: {}ms", detailList.size(), (System.currentTimeMillis() - startTime));
        } catch (Exception e) {
            log.error("处理天空图 MQTT 数据异常", e);
        }
    }

    private String getCountryBySystem(String satSystem) {
        if (satSystem == null) return "OTHER";
        String sys = satSystem.toUpperCase();
        if (sys.contains("BDS") || sys.contains("BEIDOU")) return "CN";
        if (sys.contains("GPS")) return "US";
        return "OTHER";
    }

    /** 卫星观测数据 DTO */
    public static class SatObsDTO {
        private String stationId;
        private String satNo;
        private String satSystem;
        private String country;
        private Double elevation;
        private Double azimuth;
        public String getStationId() { return stationId; }
        public void setStationId(String stationId) { this.stationId = stationId; }
        public String getSatNo() { return satNo; }
        public void setSatNo(String satNo) { this.satNo = satNo; }
        public String getSatSystem() { return satSystem; }
        public void setSatSystem(String satSystem) { this.satSystem = satSystem; }
        public String getCountry() { return country; }
        public void setCountry(String country) { this.country = country; }
        public Double getElevation() { return elevation; }
        public void setElevation(Double elevation) { this.elevation = elevation; }
        public Double getAzimuth() { return azimuth; }
        public void setAzimuth(Double azimuth) { this.azimuth = azimuth; }
    }
}