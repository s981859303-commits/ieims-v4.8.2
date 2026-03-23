package com.ruoyi.gnss.service;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;


/**
 * GNSS MQTT 接收服务
 */
@Service
public class GnssMqttService implements MqttCallback {

    @Autowired
    private MixedLogSplitter splitter;

    private MqttClient client;

    @Value("${gnss.mqtt.broker}")
    private String brokerUrl;

    @Value("${gnss.mqtt.topic}")
    private String topic;

    @Value("${gnss.mqtt.clientIdPrefix}")
    private String clientIdPrefix;

    private String clientId() {
        return clientIdPrefix + System.currentTimeMillis();
    }

    @PostConstruct
    public void init() {
        connect();
    }

    private void connect() {
        try {
            client = new MqttClient(brokerUrl, clientId(), new MemoryPersistence());
            MqttConnectOptions options = new MqttConnectOptions();
            options.setCleanSession(true);
            options.setAutomaticReconnect(true);
            client.setCallback(this);
            client.connect(options);
            client.subscribe(topic);
            System.out.println("✅ MQTT 连接成功！");
        } catch (MqttException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        try {
            byte[] rawBytes = message.getPayload();
            if (rawBytes == null || rawBytes.length == 0) return;
            splitter.pushData(rawBytes);
        } catch (Exception e) {
            System.err.println("MQTT 处理异常: " + e.getMessage());
        }
    }

    @Override
    public void connectionLost(Throwable cause) {
        System.out.println("⚠️ MQTT 连接断开，正在重连...");
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {}

    /**
     * 发布 JSON 数据到指定的 MQTT 主题
     */
    public void publish(String targetTopic, String payload) {
        try {
            if (client != null && client.isConnected()) {
                // 使用 QoS 0 发送高频时序数据，降低延迟和网络开销
                MqttMessage message = new MqttMessage(payload.getBytes("UTF-8"));
                message.setQos(0);
                client.publish(targetTopic, message);
            }
        } catch (Exception e) {
            System.err.println("❌ MQTT 发布失败: " + e.getMessage());
        }
    }

    @PreDestroy
    public void destroy() {
        try { if (client != null) client.close(); } catch (Exception e) {}
    }
}
