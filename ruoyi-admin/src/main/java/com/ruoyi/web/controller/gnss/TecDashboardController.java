package com.ruoyi.web.controller.gnss;

import com.ruoyi.common.core.controller.BaseController;
import com.ruoyi.common.core.domain.AjaxResult;
import com.ruoyi.common.core.page.TableDataInfo;
import com.ruoyi.ieims.gnss.domain.TecResult;
import com.ruoyi.ieims.gnss.service.ITecCalculationService;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Date;
import java.util.List;

/**
 * TEC监测大屏控制器
 *
 * @author guet_developer01
 * @date 2026-05-12
 */
@Controller
@RequestMapping("/gnss/dashboard")
public class TecDashboardController extends BaseController {

    @Autowired
    private ITecCalculationService tecCalculationService;

    /**
     * TEC监测大屏页面
     * 对应菜单请求地址：/gnss/dashboard/tec-dashboard
     */
    @RequiresPermissions("gnss:tec:view")
    @GetMapping("/tec-dashboard")
    public String dashboard(ModelMap mmap) {
        return "gnss/dashboard/tec-dashboard";
    }

    /**
     * 查询TEC结果列表
     */
    @RequiresPermissions("gnss:tec:list")
    @PostMapping("/tec/list")
    @ResponseBody
    public TableDataInfo list(@RequestParam(required = false) String stationId,
                              @RequestParam(required = false) String satNo,
                              @RequestParam(required = false) Date beginTime,
                              @RequestParam(required = false) Date endTime) {
        List<TecResult> list = tecCalculationService.selectTecResultList(stationId, satNo, beginTime, endTime);
        return getDataTable(list);
    }

    /**
     * 获取最新TEC数据（前端定时刷新）
     */
    @RequiresPermissions("gnss:tec:list")
    @GetMapping("/tec/latest")
    @ResponseBody
    public AjaxResult latest(@RequestParam String stationId,
                             @RequestParam(defaultValue = "100") int limit) {
        List<TecResult> list = tecCalculationService.getLatestTecByStation(stationId, limit);
        return AjaxResult.success(list);
    }
}