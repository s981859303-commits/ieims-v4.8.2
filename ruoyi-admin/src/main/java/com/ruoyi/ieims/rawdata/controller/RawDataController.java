package com.ruoyi.ieims.rawdata.controller;

import com.ruoyi.common.core.controller.BaseController;
import com.ruoyi.common.core.domain.AjaxResult;
import com.ruoyi.ieims.rawdata.service.IRawDataService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import java.io.File;
import java.nio.file.Files;

/**
 * 原始数据下载控制器
 */
@Controller
@RequestMapping("/ieims/rawdata")
public class RawDataController extends BaseController {

    @Autowired
    private IRawDataService rawDataService;

    /**
     * 原始数据下载页面
     */
    @GetMapping()
    public String rawData() {
        return "rawdata/rawdata";
    }

    /**
     * 获取年月目录列表
     */
    @GetMapping("/dirs")
    @ResponseBody
    public AjaxResult dirs() {
        return AjaxResult.success(rawDataService.getYearMonthDirs());
    }

    /**
     * 获取目录下的日志文件
     */
    @GetMapping("/files/{dir}")
    @ResponseBody
    public AjaxResult files(@PathVariable String dir) {
        // 安全拦截，防止路径遍历攻击
        if (dir.contains("..")) return AjaxResult.error("非法目录");
        return AjaxResult.success(rawDataService.getLogFiles(dir));
    }

    /**
     * 文件下载
     */
    @GetMapping("/download/{dir}/{file}")
    public ResponseEntity<byte[]> download(@PathVariable String dir, @PathVariable String file) throws Exception {
        if (dir.contains("..") || file.contains("..")) {
            return new ResponseEntity<>(HttpStatus.FORBIDDEN);
        }

        String path = rawDataService.getFullPath(dir, file);
        File f = new File(path);
        if (!f.exists()) return new ResponseEntity<>(HttpStatus.NOT_FOUND);

        byte[] data = Files.readAllBytes(f.toPath());
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_OCTET_STREAM);
        headers.setContentDispositionFormData("attachment", file);

        return new ResponseEntity<>(data, headers, HttpStatus.OK);
    }
}