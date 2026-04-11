package com.ruoyi.gnss.service;

import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.util.UUID;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * GNSS MQTT 服务
 *
 * 功能：
 * 1. 接收 MQTT 消息并转发给解析器
 * 2. 发布数据到 MQTT
 */
@Service
public class GnssMqttService implements MqttCallbackExtended {

    private static final Logger logger = LoggerFactory.getLogger(GnssMqttService.class);

    @Autowired(required = false)
    private MixedLogSplitter splitter;

    private MqttClient client;
    private MqttConnectOptions connectOptions;
    private String clientId;

    private final AtomicBoolean connected = new AtomicBoolean(false);
    private final AtomicInteger reconnectAttempts = new AtomicInteger(0);
    private ScheduledExecutorService reconnectScheduler;

    // --- 配置参数 ---
    @Value("${gnss.mqtt.broker}")
    private String brokerUrl;

    @Value("${gnss.mqtt.topic}")
    private String topic;

    @Value("${gnss.mqtt.clientIdPrefix:gnss-receiver-}")
    private String clientIdPrefix;

    @Value("${gnss.mqtt.username:}")
    private String username;

    @Value("${gnss.mqtt.password:}")
    private String password;

    @Value("${gnss.mqtt.connectionTimeout:30}")
    private int connectionTimeout;

    @Value("${gnss.mqtt.keepAliveInterval:60}")
    private int keepAliveInterval;

    @Value("${gnss.mqtt.maxReconnectAttempts:10}")
    private int maxReconnectAttempts;

    @Value("${gnss.mqtt.reconnectDelay:5000}")
    private int reconnectDelay;

    @Value("${gnss.mqtt.qos:0}")
    private int qos;

    @PostConstruct
    public void init() {
        this.clientId = clientIdPrefix + UUID.randomUUID().toString().substring(0, 8);
        this.reconnectScheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "MQTT-Reconnect");
            t.setDaemon(true);
            return t;
        });

        initConnectOptions();
        connect();
    }

    private void initConnectOptions() {
        connectOptions = new MqttConnectOptions();
        connectOptions.setCleanSession(true);
        connectOptions.setAutomaticReconnect(true);
        connectOptions.setConnectionTimeout(connectionTimeout);
        connectOptions.setKeepAliveInterval(keepAliveInterval);

        if (username != null && !username.isEmpty()) {
            connectOptions.setUserName(username);
        }
        if (password != null && !password.isEmpty()) {
            connectOptions.setPassword(password.toCharArray());
        }
    }

    private synchronized void connect() {
        try {
            if (client != null && client.isConnected()) {
                return;
            }

            logger.info("正在连接 MQTT Broker: {}", brokerUrl);

            client = new MqttClient(brokerUrl, clientId, new MemoryPersistence());
            client.setCallback(this);
            client.connect(connectOptions);
            client.subscribe(topic, qos);

            connected.set(true);
            reconnectAttempts.set(0);

            logger.info("MQTT 连接成功！ClientId: {}, 订阅主题: {}", clientId, topic);

        } catch (MqttException e) {
            logger.error("MQTT 连接失败: {}", e.getMessage());
            handleConnectionFailure();
        }
    }

    private void handleConnectionFailure() {
        connected.set(false);
        int attempts = reconnectAttempts.incrementAndGet();

        if (attempts <= maxReconnectAttempts) {
            long delay = Math.min(reconnectDelay * (long) Math.pow(1.5, attempts - 1), 60000);
            logger.warn("将在 {} ms 后进行第 {} 次重连...", delay, attempts);
            reconnectScheduler.schedule(this::connect, delay, TimeUnit.MILLISECONDS);
        } else {
            logger.error("已达到最大重连次数 {}", maxReconnectAttempts);
        }
    }

    @Override
    public void messageArrived(String topic, MqttMessage message) throws Exception {
        try {
            byte[] rawBytes = message.getPayload();
            if (rawBytes == null || rawBytes.length == 0) {
                return;
            }

            // 1. 从 MQTT Topic 中动态提取站点 ID
            // 例如 topic 为 "ieims/gnss/data/8900_2"，截取最后一段得到 "8900_2"
            String stationId = extractStationIdFromTopic(topic);

            logger.debug("收到 MQTT 消息，站点: {}, 长度: {} 字节", stationId, rawBytes.length);

            if (splitter != null) {
                // 2. 将站点上下文绑定到当前线程（防御性编程，配合你代码里的 StationContext）
                StationContext.runWithStation(stationId, () -> {
                    // 3. 调用带 stationId 参数的 pushData 方法
                    splitter.pushDataWithStation(stationId, rawBytes);
                });
            }

        } catch (Throwable t) {
            logger.error("MQTT 消息处理发生致命异常 (已拦截，保护监听线程): {}", t.getMessage(), t);
        }
    }

    /**
     * 辅助方法：从 Topic 中提取站点 ID
     */
    private String extractStationIdFromTopic(String topic) {
        if (topic != null && topic.contains("/")) {
            // 截取最后一个 '/' 之后的内容作为 stationId
            return topic.substring(topic.lastIndexOf('/') + 1);
        }
        // 如果解析失败，回退到默认站点
        return "default_station";
    }

    @Override
    public void connectionLost(Throwable cause) {
        connected.set(false);
        logger.warn("MQTT 连接断开: {}", cause.getMessage());
    }

    @Override
    public void deliveryComplete(IMqttDeliveryToken token) {
        logger.debug("消息投递完成");
    }

    @Override
    public void connectComplete(boolean reconnect, String serverURI) {
        connected.set(true);
        reconnectAttempts.set(0);

        if (reconnect) {
            logger.info("MQTT 重连成功！Server: {}", serverURI);
            try {
                client.subscribe(topic, qos);
            } catch (MqttException e) {
                logger.error("重新订阅失败: {}", e.getMessage());
            }
        }
    }

    /**
     * 发布消息
     */
    public boolean publish(String targetTopic, String payload) {
        if (!isConnected()) {
            logger.warn("MQTT 未连接，无法发布消息");
            return false;
        }

        try {
            MqttMessage message = new MqttMessage(payload.getBytes(StandardCharsets.UTF_8));
            message.setQos(qos);
            client.publish(targetTopic, message);
            return true;
        } catch (MqttException e) {
            logger.error("MQTT 发布失败: {}", e.getMessage());
            return false;
        }
    }

    /**
     * 检查连接状态
     */
    public boolean isConnected() {
        return client != null && client.isConnected() && connected.get();
    }

    public String getClientId() {
        return clientId;
    }

    @PreDestroy
    public void destroy() {
        logger.info("正在关闭 MQTT 服务...");

        if (reconnectScheduler != null && !reconnectScheduler.isShutdown()) {
            reconnectScheduler.shutdown();
        }

        if (client != null) {
            try {
                if (client.isConnected()) {
                    client.disconnect();
                }
                client.close();
            } catch (MqttException e) {
                logger.error("关闭 MQTT 客户端异常: {}", e.getMessage());
            }
        }

        logger.info("MQTT 服务已关闭");
    }
}
