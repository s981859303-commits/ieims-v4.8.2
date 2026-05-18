package com.ruoyi.ieims.ieimsgnssstation.controller;

import java.util.List;
import org.apache.shiro.authz.annotation.RequiresPermissions;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import com.ruoyi.common.annotation.Log;
import com.ruoyi.common.enums.BusinessType;
import com.ruoyi.ieims.ieimsgnssstation.domain.IeimsGnssStation;
import com.ruoyi.ieims.ieimsgnssstation.service.IIeimsGnssStationService;
import com.ruoyi.common.core.controller.BaseController;
import com.ruoyi.common.core.domain.AjaxResult;
import com.ruoyi.common.utils.poi.ExcelUtil;
import com.ruoyi.common.core.page.TableDataInfo;

/**
 * GNSS监测站设备信息Controller
 * 
 * @author guet
 * @date 2026-04-27
 */
@Controller
@RequestMapping("/ieimsgnssstation/ieimsgnssstation")
public class IeimsGnssStationController extends BaseController
{
    private String prefix = "ieimsgnssstation/ieimsgnssstation";

    @Autowired
    private IIeimsGnssStationService ieimsGnssStationService;

    @RequiresPermissions("ieimsgnssstation:ieimsgnssstation:view")
    @GetMapping()
    public String ieimsgnssstation()
    {
        return prefix + "/ieimsgnssstation";
    }

    /**
     * 查询GNSS监测站设备信息列表
     */
    @RequiresPermissions("ieimsgnssstation:ieimsgnssstation:list")
    @PostMapping("/list")
    @ResponseBody
    public TableDataInfo list(IeimsGnssStation ieimsGnssStation)
    {
        startPage();
        List<IeimsGnssStation> list = ieimsGnssStationService.selectIeimsGnssStationList(ieimsGnssStation);
        return getDataTable(list);
    }

    /**
     * 导出GNSS监测站设备信息列表
     */
    @RequiresPermissions("ieimsgnssstation:ieimsgnssstation:export")
    @Log(title = "GNSS监测站设备信息", businessType = BusinessType.EXPORT)
    @PostMapping("/export")
    @ResponseBody
    public AjaxResult export(IeimsGnssStation ieimsGnssStation)
    {
        List<IeimsGnssStation> list = ieimsGnssStationService.selectIeimsGnssStationList(ieimsGnssStation);
        ExcelUtil<IeimsGnssStation> util = new ExcelUtil<IeimsGnssStation>(IeimsGnssStation.class);
        return util.exportExcel(list, "GNSS监测站设备信息数据");
    }

    /**
     * 新增GNSS监测站设备信息
     */
    @RequiresPermissions("ieimsgnssstation:ieimsgnssstation:add")
    @GetMapping("/add")
    public String add()
    {
        return prefix + "/add";
    }

    /**
     * 新增保存GNSS监测站设备信息
     */
    @RequiresPermissions("ieimsgnssstation:ieimsgnssstation:add")
    @Log(title = "GNSS监测站设备信息", businessType = BusinessType.INSERT)
    @PostMapping("/add")
    @ResponseBody
    public AjaxResult addSave(IeimsGnssStation ieimsGnssStation)
    {
        return toAjax(ieimsGnssStationService.insertIeimsGnssStation(ieimsGnssStation));
    }

    /**
     * 修改GNSS监测站设备信息
     */
    @RequiresPermissions("ieimsgnssstation:ieimsgnssstation:edit")
    @GetMapping("/edit/{id}")
    public String edit(@PathVariable("id") Long id, ModelMap mmap)
    {
        IeimsGnssStation ieimsGnssStation = ieimsGnssStationService.selectIeimsGnssStationById(id);
        mmap.put("ieimsGnssStation", ieimsGnssStation);
        return prefix + "/edit";
    }

    /**
     * 修改保存GNSS监测站设备信息
     */
    @RequiresPermissions("ieimsgnssstation:ieimsgnssstation:edit")
    @Log(title = "GNSS监测站设备信息", businessType = BusinessType.UPDATE)
    @PostMapping("/edit")
    @ResponseBody
    public AjaxResult editSave(IeimsGnssStation ieimsGnssStation)
    {
        return toAjax(ieimsGnssStationService.updateIeimsGnssStation(ieimsGnssStation));
    }

    /**
     * 删除GNSS监测站设备信息
     */
    @RequiresPermissions("ieimsgnssstation:ieimsgnssstation:remove")
    @Log(title = "GNSS监测站设备信息", businessType = BusinessType.DELETE)
    @PostMapping( "/remove")
    @ResponseBody
    public AjaxResult remove(String ids)
    {
        return toAjax(ieimsGnssStationService.deleteIeimsGnssStationByIds(ids));
    }
}
