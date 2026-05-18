package com.ruoyi.web.controller.gnss;

import com.ruoyi.common.core.controller.BaseController;
import com.ruoyi.common.core.domain.AjaxResult;
import com.ruoyi.ieims.gnss.schedule.GnasSchedule;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

/**
 * GNSS大屏展示Controller
 *  /gnss/dashboard/index-s4
 * @author guet_developer01
 * @date 2026-05-09
 */
@Controller
@RequestMapping("/gnss/dashboard")
public class GnssDashboardController extends BaseController {

    @Autowired
    private GnasSchedule gnasSchedule;

    /**
     * 大屏主页
     */
    @GetMapping("/index-s4")
    public String index() {
        return "gnss/dashboard/index-s4";
    }

    /**
     * 获取所有监测站概要
     */
    @GetMapping("/stations/summary")
    @ResponseBody
    public AjaxResult getStationsSummary() {
        List<Map<String, Object>> data = gnasSchedule.getAllStationsSummary();
        return success(data);
    }

    /**
     * 获取指定监测站的所有卫星数据
     */
    @GetMapping("/station/{stationId}/satellites")
    @ResponseBody
    public AjaxResult getStationSatellites(@PathVariable("stationId") String stationId) {
        List<Map<String, Object>> data = gnasSchedule.getStationLatestS4Data(stationId);
        return success(data);
    }

    /**
     * 获取指定卫星的S4趋势
     */
    @GetMapping("/satellite/trend")
    @ResponseBody
    public AjaxResult getSatelliteTrend(@RequestParam("stationId") String stationId,
                                        @RequestParam("satNo") String satNo,
                                        @RequestParam(value = "hours", defaultValue = "24") int hours) {
        Map<String, Object> data = gnasSchedule.getSatelliteS4Trend(stationId, satNo, hours);
        return success(data);
    }
}