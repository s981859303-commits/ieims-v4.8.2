package com.ruoyi.test;

import com.ruoyi.user.comm.core.redis.RedisUtil;
import com.ruoyi.user.comm.core.tdengine.TDengineUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.concurrent.ThreadLocalRandom;

@Component
@Slf4j
public class TDengineTest {

    @Autowired
    private TDengineUtil tDengineUtil;



}