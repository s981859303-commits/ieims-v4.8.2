package com.ruoyi.ieims.satellitedataquery.service.impl;

import cn.hutool.core.date.DateUtil;
import cn.hutool.core.util.StrUtil;
import com.alibaba.fastjson.JSON;
import com.ruoyi.common.core.domain.AjaxResult;
import com.ruoyi.common.core.text.Convert;
import com.ruoyi.common.utils.DateUtils;
import com.ruoyi.common.utils.ShiroUtils;
import com.ruoyi.ieims.satellitedataquery.domain.StSatObs;
import com.ruoyi.ieims.satellitedataquery.mapper.StSatObsMapper;
import com.ruoyi.ieims.satellitedataquery.service.IStSatObsService;
import com.ruoyi.user.comm.core.tdengine.TDengineUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 卫星观测数据Service业务层处理
 *
 * @author guet_developer01
 * @date 2026-04-26
 */
@Service
public class StSatObsServiceImpl implements IStSatObsService
{
    private static final Logger log = LoggerFactory.getLogger(StSatObsServiceImpl.class);

    @Autowired
    private StSatObsMapper stSatObsMapper;

    @Autowired
    private TDengineUtil tdengineUtil;

    /**
     * 查询卫星观测数据列表（从TDengine）
     *
     * @param stSatObs 查询条件
     * @return 卫星观测数据集合
     */
    @Override
    public List<StSatObs> selectStSatObsList(StSatObs stSatObs)
    {
        long startQueryTime = System.currentTimeMillis();

        // 参数校验：站点ID必填
        if (stSatObs == null || StrUtil.isBlank(stSatObs.getStationId()))
        {
            log.warn("站点ID不能为空");
            return Collections.emptyList();
        }

        // 构建TDengine查询SQL
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT ts, obs_unique_key, data_source, is_complete, local_timestamp, ")
                .append("date_source, date_from_zda, observation_time, elevation, azimuth, snr, ")
                .append("pseudorange_p1, pseudorange_p2, phase_l1, phase_p2, c1, c2, ")
                .append("station_id, sat_no, sat_system ")
                .append("FROM st_sat_obs ")
                .append("WHERE station_id = '").append(escapeSql(stSatObs.getStationId())).append("'");

        // 添加卫星ID条件
        if (StrUtil.isNotBlank(stSatObs.getSatNo()))
        {
            sqlBuilder.append(" AND sat_no = '").append(escapeSql(stSatObs.getSatNo())).append("'");
        }

        // 添加数据来源条件
        if (StrUtil.isNotBlank(stSatObs.getDataSource()))
        {
            sqlBuilder.append(" AND data_source = '").append(escapeSql(stSatObs.getDataSource())).append("'");
        }

        // 添加日期来源条件
        if (StrUtil.isNotBlank(stSatObs.getDateSource()))
        {
            sqlBuilder.append(" AND date_source = '").append(escapeSql(stSatObs.getDateSource())).append("'");
        }

        // 添加时间范围条件
        Date queryStartTime = stSatObs.getQueryStartTime();
        Date queryEndTime = stSatObs.getQueryEndTime();

        // 实时数据模式：默认查询最近5分钟
        Boolean isRealtime = stSatObs.getIsRealtime();
        if (isRealtime != null && isRealtime)
        {
            Date now = new Date();
            Date fiveMinutesAgo = DateUtils.addMinutes(now, -5);
            sqlBuilder.append(" AND ts >= '").append(DateUtils.parseDateToStr(DateUtils.YYYY_MM_DD_HH_MM_SS, fiveMinutesAgo)).append("'");
            sqlBuilder.append(" AND ts <= '").append(DateUtils.parseDateToStr(DateUtils.YYYY_MM_DD_HH_MM_SS, now)).append("'");
            log.info("实时数据模式：查询最近5分钟数据");
        }
        else
        {
            if (queryStartTime != null)
            {
                sqlBuilder.append(" AND ts >= '").append(DateUtils.parseDateToStr(DateUtils.YYYY_MM_DD_HH_MM_SS, queryStartTime)).append("'");
            }
            if (queryEndTime != null)
            {
                sqlBuilder.append(" AND ts <= '").append(DateUtils.parseDateToStr(DateUtils.YYYY_MM_DD_HH_MM_SS, queryEndTime)).append("'");
            }
        }

        // 排序和限量
        Integer limit = stSatObs.getQueryLimit();
        if (limit == null || limit <= 0 || limit > 100)
        {
            limit = 100;
        }
        sqlBuilder.append(" ORDER BY ts DESC LIMIT ").append(limit);

        String sql = sqlBuilder.toString();
        log.info("执行TDengine查询SQL: {}", sql);

        List<StSatObs> resultList = Collections.emptyList();
        try
        {
            // 执行查询
            List<Map<String, Object>> queryResult = tdengineUtil.queryForList(sql);
            if (queryResult != null && !queryResult.isEmpty())
            {
                resultList = queryResult.stream().map(this::convertMapToStSatObs).collect(Collectors.toList());
            }

            long queryDuration = System.currentTimeMillis() - startQueryTime;
            log.info("查询完成，返回{}条记录，耗时{}ms", resultList.size(), queryDuration);

            // 异步保存查询记录到MySQL（不阻塞主流程）
//            saveQueryRecordAsync(stSatObs, resultList.size(), queryDuration);
        }
        catch (Exception e)
        {
            log.error("TDengine查询失败: sql={}", sql, e);
            return Collections.emptyList();
        }

        return resultList;
    }

    /**
     * 导出卫星观测数据
     *
     * @param stSatObs 查询条件
     * @return 导出文件路径
     */
    @Override
    public String exportStSatObs(StSatObs stSatObs)
    {
        // TODO: 实现导出功能（可使用RuoYi的ExcelUtil）
        log.info("导出卫星观测数据，查询条件: stationId={}", stSatObs.getStationId());
        return "导出功能待实现";
    }

    /**
     * Map转实体对象
     */
    private StSatObs convertMapToStSatObs(Map<String, Object> map)
    {
        StSatObs obs = new StSatObs();

        if (map.containsKey("ts") && map.get("ts") != null)
        {
            Object tsObj = map.get("ts");
            if (tsObj instanceof Date)
            {
                obs.setTs((Date) tsObj);
            }
            else if (tsObj instanceof Timestamp)
            {
                obs.setTs(new Date(((Timestamp) tsObj).getTime()));
            }
        }

        obs.setObsUniqueKey(getStringValue(map, "obs_unique_key"));
        obs.setDataSource(getStringValue(map, "data_source"));

        Object isCompleteObj = map.get("is_complete");
        if (isCompleteObj != null)
        {
            if (isCompleteObj instanceof Boolean)
            {
                obs.setIsComplete((Boolean) isCompleteObj);
            }
            else if (isCompleteObj instanceof Number)
            {
                obs.setIsComplete(((Number) isCompleteObj).intValue() == 1);
            }
        }

        obs.setLocalTimestamp(getLongValue(map, "local_timestamp"));
        obs.setDateSource(getStringValue(map, "date_source"));

        Object dateFromZdaObj = map.get("date_from_zda");
        if (dateFromZdaObj != null)
        {
            if (dateFromZdaObj instanceof Boolean)
            {
                obs.setDateFromZda((Boolean) dateFromZdaObj);
            }
            else if (dateFromZdaObj instanceof Number)
            {
                obs.setDateFromZda(((Number) dateFromZdaObj).intValue() == 1);
            }
        }

        obs.setObservationTime(getStringValue(map, "observation_time"));
        obs.setElevation(getDoubleValue(map, "elevation"));
        obs.setAzimuth(getDoubleValue(map, "azimuth"));
        obs.setSnr(getDoubleValue(map, "snr"));
        obs.setPseudorangeP1(getDoubleValue(map, "pseudorange_p1"));
        obs.setPseudorangeP2(getDoubleValue(map, "pseudorange_p2"));
        obs.setPhaseL1(getDoubleValue(map, "phase_l1"));
        obs.setPhaseP2(getDoubleValue(map, "phase_p2"));
        obs.setC1(getStringValue(map, "c1"));
        obs.setC2(getStringValue(map, "c2"));
        obs.setStationId(getStringValue(map, "station_id"));
        obs.setSatNo(getStringValue(map, "sat_no"));
        obs.setSatSystem(getStringValue(map, "sat_system"));

        return obs;
    }

    /**
     * 获取字符串值
     */
    private String getStringValue(Map<String, Object> map, String key)
    {
        Object value = map.get(key);
        return value != null ? value.toString() : null;
    }

    /**
     * 获取Long值
     */
    private Long getLongValue(Map<String, Object> map, String key)
    {
        Object value = map.get(key);
        if (value == null)
        {
            return null;
        }
        if (value instanceof Number)
        {
            return ((Number) value).longValue();
        }
        try
        {
            return Long.parseLong(value.toString());
        }
        catch (NumberFormatException e)
        {
            return null;
        }
    }

    /**
     * 获取Double值
     */
    private Double getDoubleValue(Map<String, Object> map, String key)
    {
        Object value = map.get(key);
        if (value == null)
        {
            return null;
        }
        if (value instanceof Number)
        {
            return ((Number) value).doubleValue();
        }
        try
        {
            return Double.parseDouble(value.toString());
        }
        catch (NumberFormatException e)
        {
            return null;
        }
    }

    /**
     * 异步保存查询记录
     */
    private void saveQueryRecordAsync(StSatObs stSatObs, int resultCount, long queryDuration)
    {
        try
        {
            Map<String, Object> record = new HashMap<>();

            // 构建查询参数JSON
            Map<String, Object> paramsMap = new HashMap<>();
            paramsMap.put("stationId", stSatObs.getStationId());
            paramsMap.put("satNo", stSatObs.getSatNo());
            paramsMap.put("dataSource", stSatObs.getDataSource());
            paramsMap.put("dateSource", stSatObs.getDateSource());
            paramsMap.put("queryStartTime", stSatObs.getQueryStartTime());
            paramsMap.put("queryEndTime", stSatObs.getQueryEndTime());
            paramsMap.put("queryLimit", stSatObs.getQueryLimit());
            paramsMap.put("isRealtime", stSatObs.getIsRealtime());

            record.put("queryParams", JSON.toJSONString(paramsMap));
            record.put("resultCount", resultCount);
            record.put("queryDuration", queryDuration);
            record.put("stationId", stSatObs.getStationId());
            record.put("satNo", stSatObs.getSatNo());
            record.put("dataSource", stSatObs.getDataSource());
            record.put("dateSource", stSatObs.getDateSource());
            record.put("startTime", stSatObs.getQueryStartTime());
            record.put("endTime", stSatObs.getQueryEndTime());
            record.put("limitNum", stSatObs.getQueryLimit() != null ? stSatObs.getQueryLimit() : 100);
            record.put("isRealtime", (stSatObs.getIsRealtime() != null && stSatObs.getIsRealtime()) ? "1" : "0");
            record.put("createBy", ShiroUtils.getLoginName());
            record.put("remark", "卫星观测数据查询");

            stSatObsMapper.insertQueryRecord(record);
        }
        catch (Exception e)
        {
            log.error("保存查询记录失败", e);
        }
    }

    /**
     * SQL注入防护：转义单引号
     */
    private String escapeSql(String input)
    {
        if (input == null)
        {
            return null;
        }
        return input.replace("'", "''");
    }
}