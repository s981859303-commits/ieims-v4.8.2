package com.ruoyi.gnss.recorder;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableScheduling;

/**
 * GNSS 录制服务配置类
 * 功能说明:
 * - 启用配置属性绑定
 * - 显式创建配置属性 Bean
 * - 支持条件化加载
 */
@Configuration
@EnableScheduling
public class GnssRecorderConfig {

    /**
     * 创建配置属性 Bean
     * 使用 @Bean 方式确保 Bean 正确创建
     */
    @Bean
    public GnssRecorderProperties gnssRecorderProperties() {
        return new GnssRecorderProperties();
    }
}
