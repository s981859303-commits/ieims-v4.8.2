package com.ruoyi.web.controller.gnss;

import com.ruoyi.common.core.controller.BaseController;
import com.ruoyi.common.core.domain.AjaxResult;
import com.ruoyi.ieims.util.PhaseUtil;
import com.ruoyi.ieims.gnss.domain.PhaseScintillation;
import com.ruoyi.ieims.gnss.service.IPhaseScintillationService;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * 相位闪烁指数Controller
 *  /gnss/phase/dashboard
 * @author guet_developer01
 * @date 2026-05-11
 */
@Controller
@RequestMapping("/gnss/phase")
public class PhaseController extends BaseController {

    @Autowired
    private IPhaseScintillationService phaseScintillationService;

    /**
     * 大屏页面
     */
    @GetMapping("/dashboard")
    @RequiresPermissions("gnss:phase:view")
    public String dashboard() {
        return "gnss/dashboard/phase-dashboard";
    }

    /**
     * 获取汇总数据（用于大屏初始化）
     */
    @GetMapping("/summary")
    @ResponseBody
    public AjaxResult getSummary() {
        PhaseUtil.ScintillationSummary summary = phaseScintillationService.getScintillationSummary();
        return AjaxResult.success(summary);
    }

    /**
     * 获取最新数据列表
     */
    @GetMapping("/latest")
    @ResponseBody
    public AjaxResult getLatest(@RequestParam(defaultValue = "100") int limit) {
        List<PhaseScintillation> list = phaseScintillationService.selectLatestData(limit);
        return AjaxResult.success(list);
    }

    /**
     * 按监测站查询
     */
    @GetMapping("/station/{stationId}")
    @ResponseBody
    public AjaxResult getByStation(@PathVariable String stationId,
                                   @RequestParam(defaultValue = "100") int limit) {
        List<PhaseScintillation> list = phaseScintillationService.selectLatestByStation(stationId, limit);
        return AjaxResult.success(list);
    }
}