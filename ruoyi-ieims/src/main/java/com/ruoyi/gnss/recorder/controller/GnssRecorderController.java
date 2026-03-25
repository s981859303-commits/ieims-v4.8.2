package com.ruoyi.gnss.recorder.controller;

import com.ruoyi.common.core.domain.AjaxResult;
import com.ruoyi.gnss.recorder.GnssContinuousRecorderService;
import com.ruoyi.gnss.recorder.RecorderHealthStatus;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

/**
 * GNSS 录制服务监控控制器
 * 功能说明:
 * - 提供录制服务健康检查接口
 * - 提供录制服务状态查询接口
 * - 提供统计数据查询接口
 */
@RestController
@RequestMapping("/gnss/recorder")
@ConditionalOnProperty(name = "gnss.recorder.enabled", havingValue = "true")
public class GnssRecorderController {

    @Autowired
    private GnssContinuousRecorderService recorderService;

    /**
     * 健康检查接口
     *
     * @return 健康状态
     */
    @GetMapping("/health")
    public AjaxResult health() {
        RecorderHealthStatus status = recorderService.getHealthStatus();

        Map<String, Object> data = new HashMap<>();
        data.put("running", status.isRunning());
        data.put("healthy", status.isHealthy());
        data.put("interrupted", status.isInterrupted());
        data.put("diskSpaceLow", status.isDiskSpaceLow());
        data.put("timeSinceLastData", status.getTimeSinceLastData());

        if (status.isInterrupted()) {
            data.put("interruptionDuration", status.getCurrentInterruptionDuration());
            data.put("interruptionReason", status.getLastInterruptionReason());
        }

        return AjaxResult.success(data);
    }

    /**
     * 获取完整状态
     *
     * @return 完整状态信息
     */
    @GetMapping("/status")
    public AjaxResult status() {
        RecorderHealthStatus status = recorderService.getHealthStatus();

        Map<String, Object> data = new HashMap<>();

        // 服务状态
        data.put("running", status.isRunning());
        data.put("healthy", status.isHealthy());
        data.put("runningDuration", status.getRunningDuration());
        data.put("startTime", status.getStartTime());

        // 数据统计
        data.put("rtcmPacketCount", status.getRtcmPacketCount());
        data.put("nmeaPacketCount", status.getNmeaPacketCount());
        data.put("totalBytesReceived", status.getTotalBytesReceived());
        data.put("totalBytesWritten", status.getTotalBytesWritten());

        // 文件状态
        data.put("currentFilePath", status.getCurrentFilePath());
        data.put("currentFileSize", status.getCurrentFileSize());
        data.put("currentFileDate", status.getCurrentFileDate());
        data.put("fileSwitchCount", status.getFileSwitchCount());
        data.put("daySwitchCount", status.getDaySwitchCount());

        // 中断状态
        data.put("interrupted", status.isInterrupted());
        data.put("interruptionCount", status.getInterruptionCount());
        data.put("totalInterruptionTime", status.getTotalInterruptionTime());
        data.put("maxInterruptionTime", status.getMaxInterruptionTime());
        data.put("timeSinceLastData", status.getTimeSinceLastData());

        // 错误状态
        data.put("writeFailureCount", status.getWriteFailureCount());
        data.put("lastError", status.getLastError());
        data.put("lastErrorTime", status.getLastErrorTime());
        data.put("diskSpaceLow", status.isDiskSpaceLow());

        return AjaxResult.success(data);
    }

    /**
     * 获取统计数据
     *
     * @return 统计数据
     */
    @GetMapping("/statistics")
    public AjaxResult statistics() {
        RecorderHealthStatus status = recorderService.getHealthStatus();

        Map<String, Object> data = new HashMap<>();
        data.put("rtcmPackets", status.getRtcmPacketCount());
        data.put("nmeaPackets", status.getNmeaPacketCount());
        data.put("bytesReceived", status.getTotalBytesReceived());
        data.put("bytesWritten", status.getTotalBytesWritten());
        data.put("writeFailures", status.getWriteFailureCount());
        data.put("fileSwitches", status.getFileSwitchCount());
        data.put("daySwitches", status.getDaySwitchCount());
        data.put("interruptions", status.getInterruptionCount());
        data.put("totalInterruptionTime", status.getTotalInterruptionTime());
        data.put("maxInterruptionTime", status.getMaxInterruptionTime());

        return AjaxResult.success(data);
    }

    /**
     * 获取当前文件信息
     *
     * @return 当前文件信息
     */
    @GetMapping("/current-file")
    public AjaxResult currentFile() {
        RecorderHealthStatus status = recorderService.getHealthStatus();

        Map<String, Object> data = new HashMap<>();
        data.put("path", status.getCurrentFilePath());
        data.put("size", status.getCurrentFileSize());
        data.put("date", status.getCurrentFileDate());

        return AjaxResult.success(data);
    }

    /**
     * 获取详细状态字符串
     *
     * @return 详细状态
     */
    @GetMapping("/detail")
    public AjaxResult detail() {
        return AjaxResult.success(recorderService.getStatistics());
    }
}
