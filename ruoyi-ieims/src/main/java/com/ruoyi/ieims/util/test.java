package com.ruoyi.ieims.util;

import com.alibaba.fastjson.JSONObject;
import com.ruoyi.user.comm.core.mqtt.MqttSubscribe;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@Slf4j
public class test {
    @Autowired
    private MqttClient mqttClient;
    @MqttSubscribe(topic = "ieims/test1")
    public void handleSensorMessage(String msgContent) {
        // 解析JSON格式的传感器消息
        JSONObject json = JSONObject.parseObject(msgContent);
        String deviceId = json.getString("deviceId"); // 设备ID
        double temperature = json.getDouble("temperature"); // 温度值
        // 业务日志输出：打印解析后的传感器数据
        log.info("！！！！！！！！！！！！测试发布");
    }

}
