package com.ruoyi.gnss.service;

import com.ruoyi.gnss.domain.NmeaRecord;

/**
 * NMEA 数据存储接口
 * 作用：定义数据存储的标准。
 */
public interface INmeaStorageService {

    /**
     * 保存 NMEA 数据
     * @param record 封装好的 NMEA 数据对象
     * @return 是否保存成功
     */
    boolean saveNmeaData(NmeaRecord record);

}