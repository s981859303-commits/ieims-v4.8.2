package com.ruoyi.web.controller.gnss;

import com.ruoyi.common.core.controller.BaseController;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * 载噪比(SNR)实时监控大屏控制器
 *
 * @author guet_developer01
 * @date 2026-05-12
 */
@Controller
@RequestMapping("/gnss/dashboard/snr")
public class SnrDashboardController extends BaseController {

    @RequiresPermissions("gnss:snr:dashboard:view")
    @GetMapping
    public String dashboard() {
        return "gnss/dashboard/snr-dashboard";
    }
}