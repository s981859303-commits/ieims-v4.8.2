package com.ruoyi.gnss.service;

import com.ruoyi.gnss.domain.NmeaRecord;

/**
 * NMEA 数据存储接口
 * 作用：定义数据存储的标准。
 * 目前还没有数据库，我们先写个 Mock 实现；以后建好库了，写个 MySQL 实现替换即可。
 */
public interface INmeaStorageService {

    /**
     * 保存 NMEA 数据
     * @param record 封装好的 NMEA 数据对象
     * @return 是否保存成功
     */
    boolean saveNmeaData(NmeaRecord record);

}