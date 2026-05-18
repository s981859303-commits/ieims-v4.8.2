package com.ruoyi.ieims.gnss.mqtt;

import com.alibaba.fastjson.JSON;
import com.ruoyi.ieims.gnss.domain.GnssSolutionData;
import com.ruoyi.ieims.gnss.service.IGnssStationService;
import com.ruoyi.user.comm.core.mqtt.MqttSubscribe;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * GNSS数据MQTT监听处理器
 *
 * @author guet_developer01
 * @date 2026-04-26
 */
@Component
@Slf4j
public class GnssDataMqttListener {

    @Autowired
    private IGnssStationService gnssStationService;

    private boolean initFlag=true;

    /**
     * 监听GNSS解算数据
     * 支持通配符，可监听所有站点的GNSS解算数据
     */
    @MqttSubscribe(value = "ieims/gnss/solution/#", qos = 1)
    public void onGnssSolutionData(String topic, String payload) {
        try {
            if (initFlag) {
                initFlag=false;
                log.debug("✅收到GNSS解算数据，topic：{}，payload：{}", topic, payload);
            }

            // 1. 解析JSON数据
            GnssSolutionData data = JSON.parseObject(payload, GnssSolutionData.class);
            if (data.getStationId()==null){
                data.setStationId(topic.replace("ieims/gnss/solution/",""));
            }
            // 2. 校验必要字段
            if (data.getStationId() == null || data.getTime() == null) {
                log.warn("GNSS数据缺少必要字段，topic：{}，payload：{}", topic, payload);
                return;
            }

            // 3. 处理数据（写入TDengine、更新状态等）
            gnssStationService.handleGnssSolutionData(data);

        } catch (Exception e) {
            log.error("处理GNSS解算数据失败，topic：{}，payload：{}", topic, payload, e);
        }
    }
}