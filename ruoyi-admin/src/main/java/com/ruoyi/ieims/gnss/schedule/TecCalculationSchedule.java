package com.ruoyi.ieims.gnss.schedule;

import com.ruoyi.ieims.gnss.service.ITecCalculationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * TEC计算定时任务（每10分钟触发）
 *
 * @author guet_developer01
 * @date 2026-05-12
 */
@Component
public class TecCalculationSchedule {

    private static final Logger log = LoggerFactory.getLogger(TecCalculationSchedule.class);

    @Autowired
    private ITecCalculationService tecCalculationService;

    /**
     * 每10分钟执行：00:00, 00:10, 00:20 ...
     */
    @Scheduled(cron = "0 0/10 * * * ?")
    public void tecComputeJob() {
        log.info("===== TEC定时计算任务开始 =====");
        try {
            tecCalculationService.computeAndStoreTec();
            log.info("===== TEC定时计算任务完成 =====");
        } catch (Exception e) {
            log.error("TEC定时计算任务异常", e);
        }
    }
}