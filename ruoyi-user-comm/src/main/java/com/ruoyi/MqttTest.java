package com.ruoyi;

import com.alibaba.fastjson.JSONObject;
import com.ruoyi.user.comm.core.mqtt.MqttSubscribe;
import com.ruoyi.user.comm.core.mqtt.MqttUtil;
import com.ruoyi.user.comm.core.redis.RedisUtil;
import com.ruoyi.user.comm.core.tdengine.TDengineUtil;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.eclipse.paho.client.mqttv3.MqttMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.nio.charset.StandardCharsets;

/**
 * MQTT消息测试类
 * 功能：
 * 1. 初始化时发布测试MQTT消息
 * 2. 订阅指定MQTT主题并处理收到的消息
 * 3. 演示不同参数类型的MQTT消息处理方法
 * @author （可补充作者）
 * @date （可补充日期）
 */
@Component // 标记为Spring组件，交由容器管理
public class MqttTest {
    // 日志对象，用于输出程序运行日志
    private static final Logger log = LoggerFactory.getLogger(MqttTest.class);

    // ==================== 依赖注入 ====================
    // 注入Redis工具类（示例，暂未实际使用）
    @Autowired
    private RedisUtil redisUtil;
    // 注入MQTT工具类，用于发布MQTT消息
    @Autowired
    private MqttUtil mqttUtil;
    // 注入TDengine工具类（示例，暂未实际使用）
    @Autowired
    private TDengineUtil tdengineUtil;

    /**
     * 初始化方法：Spring容器加载完成后执行
     * 功能：异步延迟发布测试MQTT消息，确保MQTT订阅逻辑已初始化完成
     */
    @PostConstruct // Spring Bean初始化完成后执行该方法
    public void testAll() {
        // 开启新线程执行，避免阻塞Spring容器初始化
        new Thread(() -> {
            try {
                // 延迟3秒执行（原1秒调整为3秒，确保订阅逻辑完全加载）
                Thread.sleep(3000);

                // 测试1：发布传感器数据消息到指定主题
                // 主题：ieims/data/sensor，匹配@MqttSubscribe(topic = "ieims/data/sensor")
                String sensorMsg = "{\"deviceId\":\"sensor_001\",\"temperature\":25.6,\"time\":\"20260309\"}";
                mqttUtil.publish("ieims/data/sensor", sensorMsg, 1); // 发布消息，QOS=1（至少一次送达）

                // 测试2：发布通用数据消息到指定主题
                // 主题：ieims/data，匹配@MqttSubscribe(topic = "ieims/data")（当前代码中该订阅已注释，实际匹配ieims/data/#）
                mqttUtil.publish("ieims/data", "测试通用数据", 1);

                // 测试3：发布心跳消息到指定主题
                // 主题：ieims/heartbeat，匹配@MqttSubscribe("ieims/heartbeat")
                mqttUtil.publish("ieims/heartbeat", "", 1);

                // 打印日志，标记测试消息发布完成
                log.info("测试消息发布完成");
            } catch (InterruptedException e) {
                // 捕获线程休眠中断异常
                log.error("线程休眠被中断：" + e.getMessage());
                e.printStackTrace();
            } catch (MqttException e) {
                // 捕获MQTT消息发布异常
                log.error("MQTT消息发布失败：" + e.getMessage());
                e.printStackTrace();
            }
        }).start();
    }

    /**
     * MQTT消息处理方法：接收传感器数据（MqttMessage参数类型）
     * 订阅主题：ieims/data/sensor
     * @param mqttMessage MQTT原始消息对象，包含payload、QOS、是否保留等属性
     */
    @MqttSubscribe(topic = "ieims/data/sensor") // 订阅指定MQTT主题
    public void handleSensorMessage(MqttMessage mqttMessage) {
        // 将MQTT消息的二进制payload解析为UTF-8字符串
        String msgContent = new String(mqttMessage.getPayload(), StandardCharsets.UTF_8);
        // 解析JSON格式的传感器消息
        JSONObject json = JSONObject.parseObject(msgContent);
        String deviceId = json.getString("deviceId"); // 设备ID
        double temperature = json.getDouble("temperature"); // 温度值
        // 业务日志输出：打印解析后的传感器数据
        log.info("1--收到消息：【传感器消息】设备ID：" + deviceId + "，温度：" + temperature + "℃");
    }

    /**
     * MQTT消息处理方法：接收传感器数据（String参数类型）
     * 订阅主题：ieims/data/sensor（与上一个方法订阅同一主题，会同时触发）
     * @param msgContent MQTT消息内容字符串（payload解析后的结果）
     */
//    @MqttSubscribe(topic = "ieims/data/sensor") // 订阅指定MQTT主题
    public void handleSensorMessage(String msgContent) {
        // 解析JSON格式的传感器消息
        JSONObject json = JSONObject.parseObject(msgContent);
        String deviceId = json.getString("deviceId"); // 设备ID
        double temperature = json.getDouble("temperature"); // 温度值
        // 业务日志输出：打印解析后的传感器数据
        log.info("2--收到消息：【传感器消息】设备ID：" + deviceId + "，温度：" + temperature + "℃");
    }

    /**
     * MQTT消息处理方法：接收ieims/data下所有子主题的消息（主题+MqttMessage参数）
     * 订阅主题：ieims/data/#（通配符#匹配多层子主题）
     * QOS级别：1（至少一次送达）
     * @param topic 消息所属的MQTT主题
     * @param mqttMessage MQTT原始消息对象
     */
//    @MqttSubscribe(topic = "ieims/data/#", qos = 1) // 订阅主题（支持通配符），指定QOS级别
    public void handleAllDataMessage(String topic, MqttMessage mqttMessage) {
        // 将MQTT消息的二进制payload解析为UTF-8字符串
        String msgContent = new String(mqttMessage.getPayload(), StandardCharsets.UTF_8);
        // 日志输出：打印消息所属主题和内容
        log.info("收到主题：" + topic + "，内容：" + msgContent);
    }

    /**
     * MQTT消息处理方法：接收心跳消息（无参类型）
     * 订阅主题：ieims/heartbeat（简化写法，等价于topic = "ieims/heartbeat"）
     * 功能：仅监听主题，不处理消息内容
     */
    @MqttSubscribe("ieims/heartbeat") // 简化写法，订阅指定MQTT主题
    public void handleHeartbeat() {
        // 业务日志输出：标记收到心跳消息
        log.info("收到【心跳消息】IEIMS设备心跳");
    }
}