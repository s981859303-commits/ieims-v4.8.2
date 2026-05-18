package com.ruoyi.ieims.satellitedataquery.controller;

import com.ruoyi.common.annotation.Log;
import com.ruoyi.common.core.controller.BaseController;
import com.ruoyi.common.core.domain.AjaxResult;
import com.ruoyi.common.core.page.TableDataInfo;
import com.ruoyi.common.enums.BusinessType;
import com.ruoyi.common.utils.poi.ExcelUtil;
import com.ruoyi.ieims.ieimsgnssstation.domain.IeimsGnssStation;
import com.ruoyi.ieims.satellitedataquery.domain.StSatObs;
import com.ruoyi.ieims.satellitedataquery.mapper.StSatObsMapper;
import com.ruoyi.ieims.satellitedataquery.service.IStSatObsService;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.net.URLEncoder;
import java.util.List;

/**
 * 卫星观测数据Controller
 *
 * @author guet_developer01
 * @date 2026-04-26
 */
@Controller
@RequestMapping("/satellitedataquery/stsatobs")
public class StSatObsController extends BaseController
{
    private String prefix = "satellitedataquery/stsatobs";

    @Autowired
    private IStSatObsService stSatObsService;
    @Autowired
    private StSatObsMapper stSatObsMapper;

    /**
     * 跳转到卫星观测数据页面
     */
    @RequiresPermissions("satellitedataquery:stsatobs:view")
    @GetMapping()
    public String stsatobs()
    {
        return prefix + "/index";
    }

    /**
     * 查询卫星观测数据列表
     */
    @RequiresPermissions("satellitedataquery:stsatobs:query")
    @PostMapping("/list")
    @ResponseBody
    public TableDataInfo list(StSatObs stSatObs)
    {
        startPage();
        List<StSatObs> list = stSatObsService.selectStSatObsList(stSatObs);
        return getDataTable(list);
    }

    /**
     * 获取实时数据（专门用于实时数据复选框）
     */
    @RequiresPermissions("satellitedataquery:stsatobs:query")
    @PostMapping("/realtime")
    @ResponseBody
    public TableDataInfo getRealtimeData(@RequestParam("stationId") String stationId)
    {
        if (stationId == null || stationId.trim().isEmpty())
        {
            return getDataTable(null);
        }

        StSatObs stSatObs = new StSatObs();
        stSatObs.setStationId(stationId);
        stSatObs.setIsRealtime(true);
        stSatObs.setQueryLimit(100);

        List<StSatObs> list = stSatObsService.selectStSatObsList(stSatObs);
        return getDataTable(list);
    }

    /**
     * 导出卫星观测数据 - 直接返回文件流
     */
    @RequiresPermissions("satellitedataquery:stsatobs:export")
    @Log(title = "卫星观测数据", businessType = BusinessType.EXPORT)
    @PostMapping("/export")
    public void export(StSatObs stSatObs, HttpServletResponse response) throws IOException
    {
        List<StSatObs> list = stSatObsService.selectStSatObsList(stSatObs);
        ExcelUtil<StSatObs> util = new ExcelUtil<StSatObs>(StSatObs.class);

        // 设置响应头，直接输出文件流
        response.setContentType("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
        response.setCharacterEncoding("utf-8");
        String fileName = URLEncoder.encode("卫星观测数据", "UTF-8").replaceAll("\\+", "%20");
        response.setHeader("Content-disposition", "attachment;filename*=utf-8''" + fileName + ".xlsx");

        // 导出Excel到响应流
        util.exportExcel(response, list, "卫星观测数据");
    }
    /**
     * 获取站点ID和名称列表（用于下拉选择）
     */
//    @RequiresPermissions("satellitedataquery:stsatobs:query")
    @PostMapping("/getStationList")
    @ResponseBody
    public AjaxResult getStationList()
    {
        List<IeimsGnssStation> stationList = stSatObsMapper.selectAllStationIdAndName();
        return AjaxResult.success(stationList);
    }
}