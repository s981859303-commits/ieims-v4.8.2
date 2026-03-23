package com.ruoyi.gnss.service.impl;

import com.ruoyi.gnss.domain.NmeaRecord;
import com.ruoyi.gnss.service.INmeaStorageService;
import org.springframework.stereotype.Service;

/**
 * 模拟存储服务 (假数据库)
 * 等你的 MySQL 建好后，你可以删除这个类，或者新建一个 MysqlNmeaStorageService
 */
@Service
public class MockNmeaStorageService implements INmeaStorageService {

    @Override
    public boolean saveNmeaData(NmeaRecord record) {
        // 模拟入库操作：直接打印在控制台
        System.out.println("💾 [模拟数据库] 正在插入数据...");
        System.out.println("   -> 时间: " + record.getReceivedTime());
        System.out.println("   -> 内容: " + record.getRawContent());

        // 假装插入成功
        return true;
    }
}