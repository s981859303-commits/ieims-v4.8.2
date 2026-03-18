package com.ruoyi;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.scheduling.annotation.EnableScheduling;

import java.awt.*;
import java.net.URI;

/**
 * 启动程序
 * 
 * @author ruoyi
 */
@EnableScheduling // 开启定时任务核心注解
@SpringBootApplication(exclude = { DataSourceAutoConfiguration.class })
public class RuoYiApplication implements ApplicationRunner {
    // 从application.yml中读取配置，如果没有配置，使用默认值
    @Value("${ruoyi.sysName:电离层环境智能监测与服务系统}")
    private String sysName;
    public static void main(String[] args) {
        SpringApplication.run(RuoYiApplication.class, args);
    }

    @Override
    public void run(ApplicationArguments args) {
        System.out.println(sysName + "启动成功。点击可在浏览器中打开：http://localhost/\n");
        //在浏览器中打开：http://localhost/

    }
}