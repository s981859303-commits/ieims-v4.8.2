package com.ruoyi.framework.web.service;

import com.ruoyi.common.utils.http.HttpUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.ruoyi.system.service.ISysConfigService;

/**
 * RuoYi首创 html调用 thymeleaf 实现参数管理
 * 
 * @author ruoyi
 */
@Service("config")
public class ConfigService
{
    @Autowired
    private ISysConfigService configService;


    private static final Logger log = LoggerFactory.getLogger(ConfigService.class);
    /**
     * 根据键名查询参数配置信息
     * 
     * @param configKey 参数键名
     * @return 参数键值
     */
    public String getKey(String configKey)
    {
        String value = configService.selectConfigByKey(configKey);
        log.info("读取配置项 [{}]，值为：{}", configKey, value); // 新增日志
        return value;
//        return configService.selectConfigByKey(configKey);
    }
}
