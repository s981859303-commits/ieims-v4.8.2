package com.ruoyi.web.controller.gnss;

import com.ruoyi.common.annotation.Log;
import com.ruoyi.common.core.controller.BaseController;
import com.ruoyi.common.core.domain.AjaxResult;
import com.ruoyi.common.enums.BusinessType;
import com.ruoyi.ieims.gnss.domain.TecResult;
import com.ruoyi.ieims.gnss.service.ITecResultService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

/**
 * TEC 数据大屏 Controller
 */
@RestController
@RequestMapping("/gnss/tec")
public class TecScreenController extends BaseController {

    @Autowired
    private ITecResultService tecResultService;

    @GetMapping("/latest")
    @PreAuthorize("@ss.hasPermi('ieims:tec:screen')")
    @Log(title = "TEC大屏", businessType = BusinessType.QUERY)
    public AjaxResult getLatest(@RequestParam String stationId,
                                @RequestParam(defaultValue = "200") int limit) {
        if (stationId == null || stationId.trim().isEmpty()) {
            return AjaxResult.error("站点ID不能为空");
        }
        if (limit > 1000) limit = 1000;
        List<TecResult> list = tecResultService.selectLatestByStation(stationId.trim(), limit);
        return AjaxResult.success(list);
    }

    @GetMapping("/summary")
    @PreAuthorize("@ss.hasPermi('ieims:tec:screen')")
    public AjaxResult getSummary() {
        Map<String, Object> summary = tecResultService.selectLatestSummary();
        return AjaxResult.success(summary);
    }
}