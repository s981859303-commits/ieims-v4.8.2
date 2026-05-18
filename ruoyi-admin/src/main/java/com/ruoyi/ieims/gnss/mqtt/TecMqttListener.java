package com.ruoyi.ieims.gnss.mqtt;

import com.ruoyi.user.comm.core.mqtt.MqttSubscribe;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * TEC专用MQTT订阅监听器
 * 监听RTCM解析后的双频观测数据流
 *
 * @author guet_developer01
 * @date 2026-05-12
 */
@Slf4j
@Component
public class TecMqttListener {

    @Autowired
    private TecDataFeeder tecDataFeeder;

    /**
     * 订阅设备观测数据主题
     * 主题格式: ieims/gnss/obs/{stationId}
     */
    @MqttSubscribe(value = "ieims/gnss/obs/#", qos = 1)
    public void onGnssObsData(String topic, String payload) {
        tecDataFeeder.onObservationMessage(topic, payload);
    }
}