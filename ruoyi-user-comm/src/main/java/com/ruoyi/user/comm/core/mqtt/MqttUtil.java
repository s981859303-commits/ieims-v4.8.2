package com.ruoyi.user.comm.core.mqtt;

import com.ruoyi.user.comm.config.MqttAutoConfiguration;
import org.eclipse.paho.client.mqttv3.MqttClient;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;

/**
 * MQTT 消息操作工具类
 * <p>提供 MQTT 消息的发布、订阅、取消订阅等核心操作，基于 Eclipse Paho 客户端实现</p>
 * <p>该类已交由 Spring 容器管理，可通过 @Autowired 直接注入使用</p>
 *
 * @author （可补充作者信息）
 * @date （可补充日期）
 */
@Component
public class MqttUtil {

    private static final Logger log = LoggerFactory.getLogger(MqttUtil.class);
    /**
     * MQTT 客户端实例（由 Spring 容器注入配置好的客户端）
     */
    @Autowired
    private MqttClient mqttClient;



    /**
     * 发布指定QoS等级的MQTT消息
     *
     * @param topic   消息发布的主题，不能为空或空字符串（如："sensor/temperature"）
     * @param payload 消息内容，字符串格式，会转换为字节数组发送
     * @param qos     消息服务质量等级：
     *                <ul>
     *                <li>0：最多一次，消息发布者只管发布，不确保消息到达</li>
     *                <li>1：至少一次，确保消息到达，但可能重复</li>
     *                <li>2：恰好一次，确保消息只到达一次（最可靠但性能最差）</li>
     *                </ul>
     * @throws MqttException MQTT 客户端操作异常，常见原因：
     *                       <ul>
     *                       <li>客户端未连接到 broker</li>
     *                       <li>topic 格式不合法</li>
     *                       <li>qos 不在 0-2 范围内</li>
     *                       </ul>
     */
    public void publish(String topic, String payload, int qos) throws MqttException {
        if (mqttClient == null || !mqttClient.isConnected()) {
            log.info("mqttClient还未初始化成功！");
            return;
        }
        MqttMessage message = new MqttMessage(payload.getBytes());
        message.setQos(qos);
        mqttClient.publish(topic, message);
    }

    /**
     * 发布默认QoS等级（1）的MQTT消息
     * <p>简化版发布方法，默认使用 QoS=1（至少一次），满足大部分常规场景</p>
     *
     * @param topic   消息发布的主题，不能为空或空字符串
     * @param content 消息内容，字符串格式
     * @throws Exception 包含 MqttException（客户端操作异常）和其他可能的运行时异常
     */
    public void publish(String topic, String content) throws Exception {
        if (mqttClient == null || !mqttClient.isConnected()) {
            log.info("mqttClient还未初始化成功！");
            return;
        }
        MqttMessage message = new MqttMessage(content.getBytes());
        message.setQos(1); // 消息质量
        mqttClient.publish(topic, message);
    }

    /**
     * 发布消息（自定义QOS和是否保留）,失败会抛出异常
     */
    public void publish2(String topic, String content, int qos, boolean retained) throws Exception {
        if (mqttClient == null || !mqttClient.isConnected()) {
            log.info("mqttClient还未初始化成功！");
            return;
        }
            MqttMessage message = new MqttMessage(content.getBytes(StandardCharsets.UTF_8));
            message.setQos(qos);
            message.setRetained(retained);
        mqttClient.publish(topic, message);
    }

    /**
     * 发布消息（自定义QOS和是否保留）,失败不会抛出异常
     */
    public void publish(String topic, String content, int qos, boolean retained) {
        if (mqttClient == null || !mqttClient.isConnected()) {
            log.info("mqttClient还未初始化成功！");
            return;
        }
        try {
            if (!mqttClient.isConnected()) {
                log.error("MQTT未连接，发布消息失败");
                return;
            }
            MqttMessage message = new MqttMessage(content.getBytes(StandardCharsets.UTF_8));
            message.setQos(qos);
            message.setRetained(retained);
            mqttClient.publish(topic, message);
        } catch (MqttException e) {
            log.error("发布MQTT消息失败：主题={}, 内容={}", topic, content, e);
        }
    }
    /**
     * 订阅默认QoS等级（默认由客户端配置决定，通常为0）的MQTT主题
     * <p>简化版订阅方法，使用客户端默认的QoS等级</p>
     *
     * @param topic 要订阅的主题，支持通配符
     * @throws Exception 包含 MqttException（客户端操作异常）和其他可能的运行时异常
     */
    public void subscribe(String topic) throws Exception {
        if (mqttClient == null || !mqttClient.isConnected()) {
            log.info("mqttClient还未初始化成功！");
            return;
        }
        mqttClient.subscribe(topic);
    }

    /**
     * 订阅指定QoS等级的MQTT主题
     *
     * @param topic 要订阅的主题，支持通配符（如："sensor/#" 订阅所有sensor下的子主题）
     * @param qos   订阅的QoS等级，决定客户端接收消息的可靠性（取值0/1/2）
     * @throws MqttException MQTT 客户端操作异常，常见原因：
     *                       <ul>
     *                       <li>客户端未连接</li>
     *                       <li>topic 格式不合法</li>
     *                       <li>重复订阅同一主题且QoS不一致</li>
     *                       </ul>
     */
    public void subscribe(String topic, int qos) throws MqttException {
        if (mqttClient == null || !mqttClient.isConnected()) {
            log.info("mqttClient还未初始化成功！");
            return;
        }
        mqttClient.subscribe(topic, qos);
    }

    /**
     * 取消订阅指定的MQTT主题
     *
     * @param topic 要取消订阅的主题，需与订阅时的主题完全一致（包括通配符）
     * @throws MqttException MQTT 客户端操作异常，常见原因：
     *                       <ul>
     *                       <li>客户端未连接</li>
     *                       <li>未订阅过该主题</li>
     *                       <li>topic 格式不合法</li>
     *                       </ul>
     */
    public void unsubscribe(String topic) throws MqttException {
        if (mqttClient == null || !mqttClient.isConnected()) {
            log.info("mqttClient还未初始化成功！");
            return;
        }
        mqttClient.unsubscribe(topic);
    }


//    /**
//     * 发布消息到IEIMS MQTT
//     * @param topic 发布主题
//     * @param content 消息内容
//     */
//    public void publish(String topic, String content) {
//        publish(topic, content, 1, false);
//    }
//
}

/**
 * ====================== MqttUtil 工具类使用说明 ======================
 * 1. 依赖前提：
 *    - 确保 Spring 容器中已配置并注入 MqttClient 实例（需提前完成 broker 连接配置）
 *    - 项目已引入 Eclipse Paho 依赖（org.eclipse.paho:mqtt-client）
 *
 * 2. 基本使用示例：
 *    @Autowired
 *    private MqttUtil mqttUtil;
 *
 *    // 示例1：发布指定QoS的消息
 *    try {
 *        // 发布主题：sensor/temperature，内容：25.5℃，QoS=1
 *        mqttUtil.publish("sensor/temperature", "25.5℃", 1);
 *        System.out.println("消息发布成功");
 *    } catch (MqttException e) {
 *        System.err.println("消息发布失败：" + e.getMessage());
 *        e.printStackTrace();
 *    }
 *
 *    // 示例2：订阅主题（指定QoS）
 *    try {
 *        // 订阅主题：sensor/#（所有sensor子主题），QoS=0
 *        mqttUtil.subscribe("sensor/#", 0);
 *        System.out.println("主题订阅成功");
 *    } catch (MqttException e) {
 *        System.err.println("主题订阅失败：" + e.getMessage());
 *        e.printStackTrace();
 *    }
 *
 *    // 示例3：取消订阅
 *    try {
 *        mqttUtil.unsubscribe("sensor/#");
 *        System.out.println("取消订阅成功");
 *    } catch (MqttException e) {
 *        System.err.println("取消订阅失败：" + e.getMessage());
 *        e.printStackTrace();
 *    }
 *
 *    // 示例4：使用简化版发布/订阅（默认QoS）
 *    try {
 *        mqttUtil.publish("sensor/humidity", "60%"); // 默认QoS=1
 *        mqttUtil.subscribe("sensor/humidity");     // 默认QoS由客户端配置
 *    } catch (Exception e) {
 *        e.printStackTrace();
 *    }
 *
 * 3. 注意事项：
 *    - 所有操作前需确保 MqttClient 已成功连接到 MQTT broker，否则会抛出 MqttException
 *    - QoS 等级需与 broker 配置兼容，超出 0-2 范围会直接报错
 *    - 主题命名建议遵循规范：使用 / 分隔层级，避免特殊字符（如空格、中文符号）
 *    - 简化版 publish/subscribe 方法抛出的是 Exception 而非 MqttException，捕获时需注意
 *    - 取消订阅时，主题需与订阅时完全一致（包括通配符），否则取消无效
 *    - 建议在业务代码中统一捕获异常，避免未处理的 MqttException 导致程序中断
 */