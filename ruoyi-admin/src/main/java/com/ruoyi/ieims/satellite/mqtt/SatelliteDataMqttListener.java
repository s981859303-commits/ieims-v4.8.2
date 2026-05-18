package com.ruoyi.ieims.satellite.mqtt;

import com.alibaba.fastjson.JSON;
import com.ruoyi.ieims.satellite.domain.SatelliteObsData;
import com.ruoyi.ieims.satellite.service.ISatelliteObsService;
import com.ruoyi.user.comm.core.mqtt.MqttSubscribe;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;

/**
 * 卫星观测数据MQTT监听处理器
 *
 * @author guet_developer01
 * @date 2026-04-30
 */
@Slf4j
@Component
public class SatelliteDataMqttListener {

    @Autowired
    private ISatelliteObsService satelliteObsService;

    /**
     * 监听GNSS主题
     * 兼容性处理：由于统一使用了 ieims/gnss/obs/#
     * 必须通过 payload 的首字符判断是单个站对象还是卫星数组
     */
    @MqttSubscribe(value = "ieims/gnss/obs/#", qos = 1)
    public void onSatelliteDataReceive(String topic, String payload) {
        try {
            if (payload == null || payload.trim().isEmpty()) {
                return;
            }

            // 判断是否为 JSON 数组格式（代表底层卫星集合数据）
            if (payload.trim().startsWith("[")) {
                List<SatelliteObsData> dataList = JSON.parseArray(payload, SatelliteObsData.class);
                if (dataList != null && !dataList.isEmpty()) {
                    satelliteObsService.batchSaveToTDengine(dataList);
                }
            }
        } catch (Exception e) {
            log.error("解析卫星JSON数组数据失败，topic：{}，异常：", topic, e);
        }
    }
}