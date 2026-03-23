package com.ruoyi.gnss.service.impl;

import com.ruoyi.gnss.domain.GnssSolution;
import com.ruoyi.gnss.service.IGnssStorageService;
import org.springframework.stereotype.Service;

/**
 * 模拟存储服务 (等数据库建好后，删除这个类，或者把 @Service 注解去掉)
 */
@Service
public class MockGnssStorageServiceImpl implements IGnssStorageService {

    @Override
    public void saveSolution(GnssSolution solution) {
        // 模拟入库：直接打印出来
        System.out.println("💾 [模拟入库] " + solution.toString());

        // TODO: 等你有数据库了，就在这里调用 UserMapper.insert(solution);
    }
}