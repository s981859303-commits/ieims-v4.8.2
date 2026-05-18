package com.ruoyi.ieims.rawdata.service;

import java.util.List;

/**
 * 原始数据下载 服务层
 */
public interface IRawDataService {
    // 获取所有年月目录
    List<String> getYearMonthDirs();

    // 获取指定目录下的日志文件
    List<String> getLogFiles(String dirName);

    // 获取文件完整路径
    String getFullPath(String dirName, String fileName);
}