package com.ruoyi.gnss.service;

import com.ruoyi.gnss.domain.GnssSolution;

/**
 * GNSS 数据存储接口
 * 作用：解耦“获取数据”和“保存数据”。
 * 目前数据库没好，先用 Mock 实现；以后好了，用 MyBatis/JPA 实现即可。
 */
public interface IGnssStorageService {

    /**
     * 保存解算结果
     * @param solution 解算后的实体对象
     */
    void saveSolution(GnssSolution solution);

}