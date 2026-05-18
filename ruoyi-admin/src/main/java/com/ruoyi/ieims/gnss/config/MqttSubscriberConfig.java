package com.ruoyi.ieims.gnss.config;

import com.ruoyi.ieims.util.SnrUtil;
import com.ruoyi.ieims.util.StmUtil;
import org.eclipse.paho.client.mqttv3.MqttConnectOptions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.annotation.ServiceActivator;
import org.springframework.integration.channel.DirectChannel;
import org.springframework.integration.mqtt.core.DefaultMqttPahoClientFactory;
import org.springframework.integration.mqtt.core.MqttPahoClientFactory;
import org.springframework.integration.mqtt.inbound.MqttPahoMessageDrivenChannelAdapter;
import org.springframework.messaging.MessageChannel;
import org.springframework.messaging.MessageHandler;

@Configuration
public class MqttSubscriberConfig {

    @Autowired
    private SnrUtil snrUtil;

    @Autowired
    private StmUtil stmUtil;

    @Bean
    public MqttPahoClientFactory mqttClientFactory() {
        DefaultMqttPahoClientFactory factory = new DefaultMqttPahoClientFactory();
        MqttConnectOptions options = new MqttConnectOptions();
        options.setServerURIs(new String[] { "tcp://10.12.1.211:1883" }); // 此处改为你内网MQTT地址
        options.setUserName("admin");
        options.setPassword("Guet@90-=".toCharArray());
        options.setCleanSession(true);
        factory.setConnectionOptions(options);
        return factory;
    }

    @Bean
    public MessageChannel mqttInputChannel() {
        return new DirectChannel();
    }

    @Bean
    public MqttPahoMessageDrivenChannelAdapter inbound() {
        // 客户端ID需唯一，订阅主题：ieims/gnss/obs/#
        MqttPahoMessageDrivenChannelAdapter adapter =
                new MqttPahoMessageDrivenChannelAdapter("server_inbound_client", mqttClientFactory(), "ieims/gnss/obs/#");
        adapter.setCompletionTimeout(5000);
        adapter.setOutputChannel(mqttInputChannel());
        return adapter;
    }

    @Bean
    @ServiceActivator(inputChannel = "mqttInputChannel")
    public MessageHandler handler() {
        return message -> {
            String topic = message.getHeaders().get("mqtt_receivedTopic").toString();
            String payload = message.getPayload().toString();

            // 如果是观测数据，同时更新 SNR 和 天空图
            if (topic.startsWith("ieims/gnss/obs/")) {
                stmUtil.processMqttPayload(payload); // 更新天空图
                snrUtil.processMqttPayload(payload); // 更新SNR柱状图
            }
        };
    }
}