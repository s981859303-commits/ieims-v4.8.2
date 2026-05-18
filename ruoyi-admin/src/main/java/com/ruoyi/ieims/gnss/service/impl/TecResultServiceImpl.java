package com.ruoyi.ieims.gnss.service.impl;

import com.ruoyi.ieims.gnss.domain.TecResult;
import com.ruoyi.ieims.gnss.mapper.TecResultMapper;
import com.ruoyi.ieims.gnss.service.ITecResultService;
import com.ruoyi.user.comm.core.tdengine.TDengineUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.BeanPropertyRowMapper;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

/**
 * TEC结果数据 Service实现
 */
@Service
// 【注意：这里已经彻底不需要 @DataSource 注解了】
public class TecResultServiceImpl implements ITecResultService {

    @Autowired
    private TecResultMapper tecResultMapper;

    // 【关键1：注入你的 TDengine 工具类】
    @Autowired
    private TDengineUtil tdengineUtil;

    @Override
    public List<TecResult> selectLatestByStation(String stationId, int limit) {
        // 1. 编写查询 TDengine 的 SQL 语句
        String sql = "SELECT * FROM ieims.tec_result_data WHERE station_id = ? ORDER BY ts DESC LIMIT ?";

        // 2. 放弃使用 tecResultMapper，直接用 tdengineUtil 查询
        // BeanPropertyRowMapper 会自动把数据库里的下划线字段 (sat_no) 映射到实体类的驼峰属性 (satNo)
        return tdengineUtil.queryForEntityList(sql, new BeanPropertyRowMapper<>(TecResult.class), stationId, limit);
    }

    @Override
    public Map<String, Object> selectLatestSummary() {
        // 如果这里也涉及查 TDengine 的大屏统计，同样需要改成 tdengineUtil 的查询方式
        // 比如：
        // String sql = "SELECT COUNT(*) as total, AVG(vtec) as avgVtec FROM ieims.tec_result_data WHERE ts >= NOW - 1h";
        // return tdengineUtil.queryForList2(sql).get(0);

        // 暂时保留你原来的写法，如果你发现顶部的 4 个统计卡片也报 doesn't exist，请按照上面的注释修改这里。
        return tecResultMapper.selectLatestSummary();
    }


}