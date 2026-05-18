package com.ruoyi.ieims.rawdata.service.impl;

import com.ruoyi.ieims.rawdata.service.IRawDataService;
import com.ruoyi.system.service.ISysConfigService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

@Service
public class RawDataServiceImpl implements IRawDataService {

    // 原始数据根目录（固定路径）
    private static String rawdataPath = "D:/ieims/logdata/";
    @Autowired
    ISysConfigService iSysConfigService;
    @Override
    public List<String> getYearMonthDirs() {
        rawdataPath=getBasePath();
        File root = new File(rawdataPath);
        if (!root.exists() || !root.isDirectory()) {
            return new ArrayList<>();
        }
        // 只读取目录
        File[] dirs = root.listFiles(File::isDirectory);
        if (dirs == null) return new ArrayList<>();

        return Arrays.stream(dirs)
                .map(File::getName)
                .sorted()
                .collect(Collectors.toList());
    }
    /**
     * 获取原始数据根目录（从系统参数读取，自动补全路径分隔符 \）
     */
    private String getBasePath() {
        // 1. 从系统配置表读取
        String path = iSysConfigService.selectConfigByKey("sys.rawdataPath");

        // 2. 空值兜底
        if (path == null || path.trim().isEmpty()) {
            path = "D:\\ieims\\logdata";
        }

        // 3. 如果不以 \ 结尾，自动加上 \
        if (!path.endsWith("\\")) {
            path = path + "\\";
        }

        return path;
    }
    @Override
    public List<String> getLogFiles(String dirName) {
        File dir = new File(rawdataPath + dirName);
        if (!dir.exists() || !dir.isDirectory()) {
            return new ArrayList<>();
        }
        // 只读取.log文件
        File[] files = dir.listFiles(f -> f.isFile() && f.getName().endsWith(".log"));
        if (files == null) return new ArrayList<>();

        return Arrays.stream(files)
                .map(File::getName)
                .sorted()
                .collect(Collectors.toList());
    }

    @Override
    public String getFullPath(String dirName, String fileName) {
        return rawdataPath + dirName + "/" + fileName;
    }
}