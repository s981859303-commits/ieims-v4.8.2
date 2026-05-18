package com.ruoyi.web.controller.gnss;

import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;

/**
 * GNSS 星座图大屏控制器
 *
 * @author guet_developer01
 * @date 2026-05-12
 */
@Controller
@RequestMapping("/gnss/dashboard")
public class StmDashboardController {

    /**
     * 进入星座图大屏页面
     */
    @GetMapping("/Stm-dashboard")
    @RequiresPermissions("gnss:constellation:view")
    public String dashboard() {
        return "gnss/dashboard/Stm-dashboard";
    }
}