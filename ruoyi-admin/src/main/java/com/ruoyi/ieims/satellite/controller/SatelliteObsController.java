package com.ruoyi.ieims.satellite.controller;

import com.ruoyi.common.core.controller.BaseController;
import com.ruoyi.common.core.domain.AjaxResult;
import com.ruoyi.common.core.page.TableDataInfo;
import com.ruoyi.user.comm.core.tdengine.TDengineUtil;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.List;
import java.util.Map;

/**
 * 卫星观测数据 Controller
 * * @author guet_developer01
 * @date 2026-04-30
 */
@Controller
@RequestMapping("/ieims/satellite")
public class SatelliteObsController extends BaseController {

    private String prefix = "ieims/satellite";

    @Autowired
    private TDengineUtil tdengineUtil;

    @RequiresPermissions("ieims:satellite:view")
    @GetMapping()
    public String satellite() {
        return prefix + "/satellite";
    }

    /**
     * 从 TDengine 查询最新卫星观测数据列表
     */
    @RequiresPermissions("ieims:satellite:list")
    @PostMapping("/list")
    @ResponseBody
    public TableDataInfo list(String stationId) {
        startPage();
        // 拼接查询超级表的SQL
        String sql = "SELECT * FROM ieims.st_sat_obs WHERE 1=1 ";
        if (stationId != null && !stationId.trim().isEmpty()) {
            sql += " AND station_id = '" + stationId + "'";
        }
        sql += " ORDER BY ts DESC LIMIT 100"; // 限制返回数量保障前端渲染

        List<Map<String, Object>> list = tdengineUtil.queryForList(sql);
        return getDataTable(list);
    }
}