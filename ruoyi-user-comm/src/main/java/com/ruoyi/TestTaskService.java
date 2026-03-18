package com.ruoyi;

import com.ruoyi.user.comm.core.mqtt.MqttUtil;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.paho.client.mqttv3.MqttException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.util.Date;

/**
 * 定时任务测试服务类
 * 功能：
 * 1. 演示Quartz定时任务、Spring Schedule定时任务的使用方式
 * 2. 结合线程池异步执行耗时任务，避免定时任务线程阻塞
 * 3. 定时发布MQTT消息到指定主题，模拟传感器数据上报
 * @author （可补充作者）
 * @date （可补充日期）
 */
@Slf4j // Lombok注解，自动生成日志对象（注：与手动声明的log重复，可择一保留）
@Service // 标记为Spring服务组件，交由容器管理，支持定时任务注解扫描
public class TestTaskService {
    // 手动声明日志对象（与@Slf4j注解重复，建议保留其一，此处保留用于演示）
    // ==================== 依赖注入 ====================
    // 注入Spring线程池执行器，用于异步执行耗时任务，避免阻塞定时任务线程
    @Autowired
    private ThreadPoolTaskExecutor taskExecutor;
    // 注入MQTT工具类，用于发布MQTT消息到指定主题
    @Autowired
    private MqttUtil mqttUtil;

    /**
     * Quartz定时任务触发方法（每秒执行）
     * 注意事项：
     * 1. 方法名需与后台Quartz配置的「调用目标」字段完全一致
     * 2. 该方法为同步执行，建议仅做轻量级逻辑，耗时操作需异步处理
     */
    public void targetMethod() {
        // 打印日志：输出当前时间戳，标记任务执行
        log.info("【Quartz每秒任务】当前时间：{}，执行指定方法", System.currentTimeMillis());
        // 扩展点：此处可添加具体业务逻辑（如数据查询、计算、入库等）
    }

    /**
     * Spring Schedule固定频率定时任务（每3秒执行一次）
     * 特性：fixedRate = 3000 表示以上一次任务开始执行时间为基准，每3秒触发
     * 注意：若任务执行时间超过3秒，会出现任务堆积（下一次触发时上一次未完成）
     */
//    @Scheduled(fixedRate = 3000) // 固定频率：每3秒执行
    public void targetMethod2() {
        // 核心优化：通过线程池异步执行任务，避免阻塞定时任务线程池
        taskExecutor.execute(() -> {
            try {
                // 调用核心业务方法（异步执行，不阻塞定时任务触发）
                targetMethod();
            } catch (Exception e) {
                // 捕获任务执行异常，打印错误日志（避免线程池任务异常导致程序终止）
                log.error("【targetMethod2】异步任务执行失败", e);
            }
        });
    }

    /**
     * Spring Schedule Cron表达式定时任务（每2秒执行一次）
     * Cron规则：0/2 * * * * ? → 从0秒开始，每2秒执行一次
     * 功能：定时发布传感器JSON格式数据到MQTT主题
     */
    @Scheduled(cron = "0/5 * * * * ?") // Cron表达式：每5秒执行
    public void cronTask() {
        // 构造传感器模拟数据（JSON格式）
        String sensorMsg = "{\"deviceId\":\"sensor_001\",\"temperature\":25.6,\"time\":\"20260309\"}";
        // 发布MQTT消息：主题=ieims/data/sensor，内容=sensorMsg，QOS=1，保留消息=false
        mqttUtil.publish("ieims/data/sensor", sensorMsg, 1, false);
    }

    /**
     * Spring Schedule Cron表达式定时任务（每秒执行一次）
     * Cron规则：0/1 * * * * ? → 从0秒开始，每秒执行一次
     * 功能：定时生成随机温度数据，发布到MQTT主题
     */
//    @Scheduled(cron = "0/5 * * * * ?") // Cron表达式：每5秒执行
    public void task2() {
        // 生成20~30℃之间的随机温度，保留1位小数（格式：25.5℃）
        String msg = String.format("%.1f℃", 20 + Math.random() * 10);
        try {
            // 发布MQTT消息：主题=ieims/data，内容=随机温度，QOS=1（至少一次送达）
            mqttUtil.publish("ieims/data", msg, 1);
            // 打印成功日志，标记消息发布状态
            log.info("消息:{}发布成功", msg);
        } catch (MqttException e) {
            // 捕获MQTT发布异常，打印失败日志
            log.info("消息:{}发布失败:{}", msg, e.getMessage());
            // 打印异常堆栈，便于定位问题（生产环境可根据需要关闭）
            e.printStackTrace();
        }
    }

    /**
     * Spring Schedule Cron表达式定时任务（每秒执行一次，示例为生产环境每天23:59执行的模板）
     * Cron规则说明：
     * - 示例Cron：0/1 * * * * ? → 每秒执行（仅用于测试）
     * - 生产环境规则：0 59 23 * * ? → 每天23点59分执行
     * 功能：定时发布系统心跳消息到MQTT主题（保留消息=true）
     */
//    @Scheduled(cron = "0/1 * * * * ?") // 测试用：每秒执行；生产可改为 0 59 23 * * ?
    public void task3() {
        try {
            // 发布MQTT心跳消息：
            // 主题=ieims/heartbeat，内容=当前系统时间，QOS=1，保留消息=true（新订阅者可获取最后一条消息）
            mqttUtil.publish("ieims/heartbeat", new Date().toString(), 1, true);
        } catch (Exception e) {
            // 捕获所有异常，避免定时任务因异常终止
            log.error("【task3】心跳消息发布失败", e);
        }
    }
}