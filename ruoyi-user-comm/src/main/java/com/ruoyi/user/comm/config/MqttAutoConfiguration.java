package com.ruoyi.user.comm.config;

import com.ruoyi.user.comm.core.mqtt.MqttSubscribe;
import lombok.Data;
import org.apache.shiro.util.StringUtils;
import org.eclipse.paho.client.mqttv3.*;
import org.eclipse.paho.client.mqttv3.persist.MemoryPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeansException;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.annotation.PostConstruct;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

@Data
@Configuration
@ConfigurationProperties(prefix = "mqtt.client") // 匹配你的配置前缀
public class MqttAutoConfiguration implements ApplicationContextAware {
    private static final Logger log = LoggerFactory.getLogger(MqttAutoConfiguration.class);

    // 存储：主题 -> 处理方法（key=主题，value=Map<方法, 所属Bean>）
    private final Map<String, Map<Method, Object>> topicHandlerMap = new HashMap<>();

    // MQTT连接配置
    private String broker;
    private String username;
    private String password;
    private String clientId;
    private int connectTimeout = 30;
    private int keepAliveInterval = 60;
    private boolean automaticReconnect = true;

    // Spring上下文（通过接口注入）
    private ApplicationContext applicationContext;
    // MQTT客户端（延迟从上下文获取，避免循环依赖）
    private MqttClient mqttClient;

    // ========== 实现ApplicationContextAware接口，注入上下文 ==========
    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
        log.info("Spring上下文已注入到MqttAutoConfiguration");
    }

    // ========== 单例创建MqttClient Bean（唯一入口） ==========
    @Bean
    @ConditionalOnMissingBean
    public MqttClient mqttClient() throws MqttException {
        // 校验必填参数
        if (!StringUtils.hasText(broker)) {
            throw new IllegalArgumentException("MQTT broker地址未配置！请检查mqtt.client.broker");
        }
        // 兜底生成唯一ClientId
        if (!StringUtils.hasText(clientId)) {
            clientId = "ieims-client-" + System.currentTimeMillis();
        }

        // 1. 创建连接参数
        MqttConnectOptions options = new MqttConnectOptions();
        if (StringUtils.hasText(username)) {
            options.setUserName(username);
        }
        if (StringUtils.hasText(password)) {
            options.setPassword(password.toCharArray());
        }
        options.setConnectionTimeout(connectTimeout);
        options.setKeepAliveInterval(keepAliveInterval);
        options.setAutomaticReconnect(automaticReconnect);
        options.setCleanSession(true);

        // 2. 创建并连接客户端
        MqttClient client = new MqttClient(broker, clientId, new MemoryPersistence());
        client.connect(options);
        log.info("MQTT客户端连接成功！broker: {}, clientId: {}", broker, clientId);

        // 3. 设置消息回调（核心：分发消息到注解方法）
        client.setCallback(new MqttCallback() {
            @Override
            public void connectionLost(Throwable cause) {
                log.error("MQTT连接断开！", cause);
            }

            @Override
            public void messageArrived(String topic, MqttMessage message) throws Exception {
//                String msgContent = new String(message.getPayload(), StandardCharsets.UTF_8);
//                log.info("【MQTT原始消息】主题：{}，内容：{}", topic, msgContent);
                //转字符串后再发给分发方法
//                dispatchMessage(topic, msgContent);
                //直接传递MqttMessage对象给分发方法
                dispatchMessage(topic, message);
            }

            @Override
            public void deliveryComplete(IMqttDeliveryToken token) {
//                    log.info("消息发布成功：{}", token.getTopics()[0]);
            }
        });

        return client;
    }

    // ========== 延迟初始化：扫描注解 + 订阅Topic（解决循环依赖） ==========
    @PostConstruct
    public void initMqttSubscriber() {

        new Thread(() -> {
            // 等待订阅完成（最多等10秒）
            long timeout = 10000;
            long start = System.currentTimeMillis();
            while (applicationContext==null  && (System.currentTimeMillis() - start) < timeout) {
                try {
                    Thread.sleep(500); // 每500ms检查一次
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
            while ( (System.currentTimeMillis() - start) < timeout) {
                // 从上下文获取已创建的MqttClient Bean（核心修复：避免循环依赖）
                try {
                    this.mqttClient = applicationContext.getBean(MqttClient.class);
                    break;
                } catch (Exception e) {
                    log.error("获取MqttClient Bean失败！", e);
                }
                try {
                    Thread.sleep(500); // 每500ms检查一次
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }

            // 1. 扫描所有Bean中的@MqttSubscribe注解
            String[] beanNames = applicationContext.getBeanDefinitionNames();
            log.info("开始扫描MQTT订阅注解，共扫描{}个Bean", beanNames.length);

            for (String beanName : beanNames) {
                try {
                    Object bean = applicationContext.getBean(beanName);
                    Method[] methods = bean.getClass().getDeclaredMethods();
                    for (Method method : methods) {
                        MqttSubscribe annotation = method.getAnnotation(MqttSubscribe.class);
                        if (annotation != null) {
                            String topic = StringUtils.hasText(annotation.topic()) ? annotation.topic() : annotation.value();
                            if (!StringUtils.hasText(topic)) {
                                log.error("方法{}的@MqttSubscribe注解未指定主题", method.getName());
                                continue;
                            }
                            // 缓存订阅主题和处理方法
                            topicHandlerMap.computeIfAbsent(topic, k -> new HashMap<>()).put(method, bean);
                            log.info("发现MQTT订阅方法：{}，订阅主题：{}", method.getName(), topic);
                        }
                    }
                } catch (Exception e) {
                    log.warn("扫描Bean {}的MQTT注解失败", beanName, e);
                }
            }

            // 2. 批量订阅Topic
            if (topicHandlerMap.isEmpty()) {
                log.warn("未扫描到任何@MqttSubscribe注解方法");
                return;
            }

            for (String topic : topicHandlerMap.keySet()) {
                try {
                    Method firstMethod = topicHandlerMap.get(topic).keySet().iterator().next();
                    MqttSubscribe annotation = firstMethod.getAnnotation(MqttSubscribe.class);
                    int qos = annotation.qos();
                    // 使用上下文获取的客户端订阅
                    mqttClient.subscribe(topic, qos);
                    log.info("订阅MQTT主题成功：{}（QOS={}）", topic, qos);
                } catch (Exception e) {
                    log.error("订阅MQTT主题{}失败", topic, e);
                }
            }
        }).start();

    }

    /**
     * 消息分发：匹配订阅主题并调用处理方法
     * @param topic 消息主题
     * @param mqttMessage MQTT原始消息对象（替代原String类型的msgContent）
     */
    private void dispatchMessage(String topic, MqttMessage mqttMessage) {
        if (topicHandlerMap.isEmpty()) {
            log.warn("无MQTT订阅方法，无法分发消息");
            return;
        }

        // 提前解析消息内容（保持原有字符串参数的兼容性）
        String msgContent = new String(mqttMessage.getPayload(), StandardCharsets.UTF_8);
//        log.info("【MQTT消息分发】主题：{}，内容：{}，QOS：{}，是否保留：{}",topic, msgContent, mqttMessage.getQos(), mqttMessage.isRetained());

        topicHandlerMap.forEach((subscribeTopic, methodBeanMap) -> {
            if (matchTopic(subscribeTopic, topic)) {
//                log.info("匹配到订阅主题：{}，开始调用处理方法", subscribeTopic);
                methodBeanMap.forEach((method, bean) -> {
                    try {
                        method.setAccessible(true);
                        Class<?>[] paramTypes = method.getParameterTypes();

                        // 适配不同参数类型的处理方法（核心修改）
                        if (paramTypes.length == 0) {
                            // 无参方法：仅触发，不传递参数
                            method.invoke(bean);
                        } else if (paramTypes.length == 1) {
                            if (paramTypes[0] == String.class) {
                                // 兼容原有String参数：传递消息内容字符串
                                method.invoke(bean, msgContent);
                            } else if (paramTypes[0] == MqttMessage.class) {
                                // 新增支持：直接传递MqttMessage对象
                                method.invoke(bean, mqttMessage);
                            } else {
                                log.error("方法{}的单个参数类型不支持！仅支持String或MqttMessage", method.getName());
                            }
                        } else if (paramTypes.length == 2) {
                            if (paramTypes[0] == String.class && paramTypes[1] == String.class) {
                                // 兼容原有：主题+内容字符串
                                method.invoke(bean, topic, msgContent);
                            } else if (paramTypes[0] == String.class && paramTypes[1] == MqttMessage.class) {
                                // 新增支持：主题+MqttMessage对象
                                method.invoke(bean, topic, mqttMessage);
                            } else {
                                log.error("方法{}的双参数类型不支持！仅支持(String,String)或(String,MqttMessage)", method.getName());
                            }
                        } else {
                            log.error("方法{}参数数量不支持！仅支持：无参 / 1个参数 / 2个参数", method.getName());
                        }
                    } catch (Exception e) {
                        log.error("调用MQTT消息处理方法失败", e);
                    }
                });
            }
        });
    }

    /**
     * 消息分发：匹配订阅主题并调用处理方法
     */
    private void dispatchMessage(String topic, String msgContent) {
        if (topicHandlerMap.isEmpty()) {
            log.warn("无MQTT订阅方法，无法分发消息");
            return;
        }

        topicHandlerMap.forEach((subscribeTopic, methodBeanMap) -> {
            if (matchTopic(subscribeTopic, topic)) {
//                log.info("匹配到订阅主题：{}，开始调用处理方法", subscribeTopic);
                methodBeanMap.forEach((method, bean) -> {
                    try {
                        method.setAccessible(true);
                        Class<?>[] paramTypes = method.getParameterTypes();
                        if (paramTypes.length == 0) {
                            method.invoke(bean);
                        } else if (paramTypes.length == 1 && paramTypes[0] == String.class) {
                            method.invoke(bean, msgContent);
                        } else if (paramTypes.length == 2
                                && paramTypes[0] == String.class
                                && paramTypes[1] == String.class) {
                            method.invoke(bean, topic, msgContent);
                        } else {
                            log.error("方法{}参数不支持！仅支持：无参 / String内容 / String主题+String内容", method.getName());
                        }
                    } catch (Exception e) {
                        log.error("调用MQTT消息处理方法失败", e);
                    }
                });
            }
        });
    }

    /**
     * 修复：严格遵循MQTT 3.1.1主题匹配规则
     */
    private boolean matchTopic(String subscribeTopic, String messageTopic) {
        // 1. 完全匹配优先
        if (subscribeTopic.equals(messageTopic)) {
            return true;
        }

        // 2. 处理通配符（转换为正则表达式）
        String regex = subscribeTopic
                // + 匹配单层非空主题（不能匹配空）
                .replace("+", "[^/]+")
                // # 匹配多层主题（必须在最后，且前面是/）
                .replace("/#", "(/.+)?")
                // 转义/避免正则冲突
                .replace("/", "\\/");

        // 3. 正则匹配（开头到结尾）
        return messageTopic.matches("^" + regex + "$");
    }
}