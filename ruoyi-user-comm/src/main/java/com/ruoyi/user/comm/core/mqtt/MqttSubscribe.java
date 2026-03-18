package com.ruoyi.user.comm.core.mqtt;
import org.springframework.core.annotation.AliasFor;
import java.lang.annotation.*;

/**
 * MQTT订阅注解：标注在方法上，自动订阅指定IEIMS MQTT主题
 */
@Target({ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface MqttSubscribe {

    /**
     * 订阅的IEIMS MQTT主题（支持通配符，如ieims/data/#）
     */
    @AliasFor("topic")
    String value() default "";

    /**
     * 订阅的主题（与value二选一，优先级更高）
     */
    @AliasFor("value")
    String topic() default "";

    /**
     * QOS级别（默认1，至少一次送达）
     */
    int qos() default 1;

    /**
     * 消息编码（默认UTF-8）
     */
    String charset() default "UTF-8";
}