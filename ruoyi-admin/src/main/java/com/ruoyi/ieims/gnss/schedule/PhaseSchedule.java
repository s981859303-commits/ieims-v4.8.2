package com.ruoyi.ieims.gnss.schedule;

import com.ruoyi.ieims.gnss.service.IPhaseScintillationService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

/**
 * 相位闪烁指数定时任务
 * 每分钟执行一次，计算各监测站各卫星的相位闪烁指数
 *
 * @author guet_developer01
 * @date 2026-05-11
 */
@Component
@EnableScheduling
public class PhaseSchedule {
    private static final Logger log = LoggerFactory.getLogger(PhaseSchedule.class);

    @Autowired
    private IPhaseScintillationService phaseScintillationService;

    /**
     * 执行相位闪烁指数计算
     * Cron表达式: 0 * * * * ?  (每分钟执行一次)
     */
    @Scheduled(cron = "0 * * * * ?")
    public void executePhaseCalculation() {
        log.info("========== 相位闪烁指数定时任务开始执行 ==========");

        long startTime = System.currentTimeMillis();

        try {
            // 执行计算
            phaseScintillationService.executeCalculation();

            long endTime = System.currentTimeMillis();
            log.info("========== 相位闪烁指数定时任务执行完成, 耗时: {}ms ==========",
                    (endTime - startTime));

        } catch (Exception e) {
            log.error("相位闪烁指数定时任务执行失败", e);
        }
    }
}