package com.ruoyi.ieims.gnss.service.impl;

import com.alibaba.fastjson.JSON;
import com.ruoyi.common.utils.DateUtils;
import com.ruoyi.common.utils.StringUtils;
import com.ruoyi.ieims.util.PhaseUtil;
import com.ruoyi.ieims.gnss.domain.PhaseScintillation;
import com.ruoyi.ieims.gnss.mapper.PhaseScintillationMapper;
import com.ruoyi.ieims.gnss.service.IPhaseScintillationService;
import com.ruoyi.user.comm.core.tdengine.TDengineUtil;
import com.ruoyi.user.comm.core.websocket.WebSocketMsg;
import com.ruoyi.user.comm.core.websocket.WebSocketUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Collectors;

/**
 * 相位闪烁指数Service实现类
 *
 * @author guet_developer01
 * @date 2026-05-11
 */
@Service
public class PhaseScintillationServiceImpl implements IPhaseScintillationService {

    private static final Logger log = LoggerFactory.getLogger(PhaseScintillationServiceImpl.class);

    /** TDengine超级表名称 */
    private static final String SUPER_TABLE_NAME = "ieims.phase_scintillation";

    /** WebSocket推送主题 */
    private static final String WS_TOPIC_SCINTILLATION = "/topic/phase";

    /** 原始观测数据表名 */
    private static final String ST_SAT_OBS_TABLE = "st_sat_obs";

    @Resource
    private PhaseScintillationMapper phaseScintillationMapper;

    @Autowired
    private PhaseUtil phaseUtil;

    @Autowired
    private TDengineUtil tdengineUtil;

    @Autowired
    private WebSocketUtil webSocketUtil;

    /**
     * 初始化TDengine超级表
     */
    @PostConstruct
    public void initSuperTable() {
        try {
            // 创建数据库(如果不存在)
            String createDbSql = "CREATE DATABASE IF NOT EXISTS ieims KEEP 365 DURATION 10 BUFFER 256";
            tdengineUtil.executeDDL(createDbSql);
            log.info("TDengine数据库ieims初始化完成");

            // 创建超级表
            String createTableSql = "CREATE STABLE IF NOT EXISTS " + SUPER_TABLE_NAME + " (" +
                    "ts TIMESTAMP," +
                    "phi4 DOUBLE," +
                    "raw_sigma_phi DOUBLE," +
                    "sample_count INT," +
                    "level_code TINYINT," +
                    "level_name BINARY(32)," +
                    "calc_time BIGINT," +
                    "window_start_time BIGINT," +
                    "window_end_time BIGINT" +
                    ") TAGS (" +
                    "station_id BINARY(64)," +
                    "sat_no BINARY(10)" +
                    ")";
            tdengineUtil.executeDDL(createTableSql);
            log.info("TDengine超级表{}初始化完成", SUPER_TABLE_NAME);
        } catch (Exception e) {
            log.error("初始化TDengine超级表失败", e);
        }
    }

    @Override
    public List<PhaseUtil.PhaseResultWithLevel> executeCalculation() {
        // 计算窗口: 最近1分钟
        long endTime = System.currentTimeMillis();
        long startTime = endTime - 60 * 1000;

        log.info("开始执行相位闪烁指数计算, 时间窗口: {} - {}",
                DateUtils.dateTime(new Date(startTime)),
                DateUtils.dateTime(new Date(endTime)));

        try {
            // 1. 直接从TDengine的st_sat_obs表中获取最近1分钟内的station_id和sat_no
            String distinctSql = "SELECT DISTINCT station_id, sat_no " +
                    "FROM " + ST_SAT_OBS_TABLE + " " +
                    "WHERE ts >= " + startTime + " AND ts <= " + endTime + " " +
                    "AND phase_l1 IS NOT NULL " +
                    "ORDER BY station_id ASC, sat_no ASC";

            List<Map<String, Object>> distinctResult = tdengineUtil.queryForList(distinctSql);

            if (distinctResult == null || distinctResult.isEmpty()) {
                log.warn("未查询到需要计算的相位数据, startTime={}, endTime={}", startTime, endTime);
                return new ArrayList<>();
            }

            log.info("获取到{}个监测站-卫星组合", distinctResult.size());

            // 2. 构建PhaseInput列表
            List<PhaseUtil.PhaseInput> inputs = new ArrayList<>();

            for (Map<String, Object> item : distinctResult) {
                String stationId = String.valueOf(item.get("station_id"));
                String satNo = String.valueOf(item.get("sat_no"));

                if (StringUtils.isEmpty(stationId) || StringUtils.isEmpty(satNo)) {
                    continue;
                }

                // 3. 基于获取到的station_id和sat_no读取最近1分钟内的相位数据
                String phaseDataSql = "SELECT ts, phase_l1, local_timestamp " +
                        "FROM " + ST_SAT_OBS_TABLE + " " +
                        "WHERE station_id = '" + escapeSqlString(stationId) + "' " +
                        "AND sat_no = '" + escapeSqlString(satNo) + "' " +
                        "AND ts >= " + startTime + " AND ts <= " + endTime + " " +
                        "AND phase_l1 IS NOT NULL " +
                        "ORDER BY ts ASC";

                List<Map<String, Object>> phaseDataList = tdengineUtil.queryForList(phaseDataSql);

                if (phaseDataList == null || phaseDataList.isEmpty()) {
                    log.debug("监测站{}卫星{}无相位数据", stationId, satNo);
                    continue;
                }

                // 提取相位序列和时间戳序列
                List<Double> phaseSeries = new ArrayList<>();
                List<Long> timestamps = new ArrayList<>();

                for (Map<String, Object> data : phaseDataList) {
                    Object phaseObj = data.get("phase_l1");
                    Object tsObj = data.get("ts");

                    if (phaseObj != null && tsObj != null) {
                        // 修正：phase_l1 转换为 Double
                        double phase = 0.0;
                        if (phaseObj instanceof Number) {
                            phase = ((Number) phaseObj).doubleValue();
                        } else if (phaseObj instanceof String) {
                            try {
                                phase = Double.parseDouble((String) phaseObj);
                            } catch (NumberFormatException e) {
                                log.warn("解析phase_l1失败: {}", phaseObj);
                                continue;
                            }
                        } else {
                            log.warn("未知的phase_l1类型: {}", phaseObj.getClass().getName());
                            continue;
                        }

                        // 修正：ts 转换为 Long (Timestamp 类型处理)
                        long ts = 0L;
                        if (tsObj instanceof Number) {
                            ts = ((Number) tsObj).longValue();
                        } else if (tsObj instanceof java.sql.Timestamp) {
                            ts = ((java.sql.Timestamp) tsObj).getTime();
                        } else if (tsObj instanceof java.util.Date) {
                            ts = ((java.util.Date) tsObj).getTime();
                        } else if (tsObj instanceof String) {
                            try {
                                ts = Long.parseLong((String) tsObj);
                            } catch (NumberFormatException e) {
                                log.warn("解析ts失败: {}", tsObj);
                                continue;
                            }
                        } else {
                            log.warn("未知的ts类型: {}", tsObj.getClass().getName());
                            continue;
                        }

                        phaseSeries.add(phase);
                        timestamps.add(ts);
                    }
                }

                if (phaseSeries.size() < 2) {
                    log.debug("监测站{}卫星{}相位数据样本数不足2, count={}",
                            stationId, satNo, phaseSeries.size());
                    continue;
                }

                // 构建PhaseInput
                PhaseUtil.PhaseInput input = new PhaseUtil.PhaseInput();
                input.setStationId(stationId);
                input.setSatNo(satNo);
                input.setPhaseSeries(phaseSeries);
                input.setTimestamps(timestamps);
                input.setStartTime(startTime);
                input.setEndTime(endTime);
                inputs.add(input);
            }

            // 4. 批量计算相位闪烁指数
            List<PhaseUtil.PhaseResultWithLevel> results = phaseUtil.batchCalculate(inputs);
            log.info("成功计算{}个相位闪烁指数", results.size());

            if (results.isEmpty()) {
                return results;
            }

            // 5. 保存到TDengine
            List<PhaseScintillation> saveList = new ArrayList<>();
            for (PhaseUtil.PhaseResultWithLevel result : results) {
                PhaseScintillation entity = convertToEntity(result);
                saveList.add(entity);
            }

            int[] saveResults = batchSaveToTDengine(saveList);
            log.info("TDengine存储完成, 成功存储{}条记录",
                    Arrays.stream(saveResults).filter(r -> r > 0).count());

            // 6. 构建汇总数据并通过WebSocket推送
            PhaseUtil.ScintillationSummary summary = phaseUtil.buildSummary(results);
            pushToWebSocket(summary);

            return results;

        } catch (Exception e) {
            log.error("执行相位闪烁指数计算失败", e);
            return new ArrayList<>();
        }
    }

    @Override
    public int saveToTDengine(PhaseScintillation entity) {
        if (entity == null) {
            return 0;
        }

        // 创建子表(如果不存在)
        String tableName = getChildTableName(entity.getStationId(), entity.getSatNo());
        String createTableSql = String.format(
                "CREATE TABLE IF NOT EXISTS %s USING %s TAGS ('%s', '%s')",
                tableName, SUPER_TABLE_NAME, entity.getStationId(), entity.getSatNo());
        tdengineUtil.executeDDL(createTableSql);

        // 插入数据
        String insertSql = String.format(
                "INSERT INTO %s (ts, phi4, raw_sigma_phi, sample_count, level_code, level_name, calc_time, window_start_time, window_end_time) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                tableName);

        Object[] params = {
                entity.getTs(),
                entity.getPhi4(),
                entity.getRawSigmaPhi(),
                entity.getSampleCount(),
                entity.getLevelCode(),
                entity.getLevelName(),
                entity.getCalcTime(),
                entity.getWindowStartTime(),
                entity.getWindowEndTime()
        };

        return tdengineUtil.executeUpdate(insertSql, params);
    }

    @Override
    public int[] batchSaveToTDengine(List<PhaseScintillation> results) {
        if (results == null || results.isEmpty()) {
            return new int[0];
        }

        // 按子表分组
        Map<String, List<PhaseScintillation>> groupMap = results.stream()
                .collect(Collectors.groupingBy(r -> getChildTableName(r.getStationId(), r.getSatNo())));

        List<int[]> batchResults = new ArrayList<>();

        for (Map.Entry<String, List<PhaseScintillation>> entry : groupMap.entrySet()) {
            String tableName = entry.getKey();
            List<PhaseScintillation> groupResults = entry.getValue();

            // 确保子表存在
            String createTableSql = String.format(
                    "CREATE TABLE IF NOT EXISTS %s USING %s TAGS ('%s', '%s')",
                    tableName, SUPER_TABLE_NAME,
                    groupResults.get(0).getStationId(),
                    groupResults.get(0).getSatNo());
            tdengineUtil.executeDDL(createTableSql);

            // 批量插入
            String insertSql = String.format(
                    "INSERT INTO %s (ts, phi4, raw_sigma_phi, sample_count, level_code, level_name, calc_time, window_start_time, window_end_time) " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    tableName);

            List<Object[]> batchParams = new ArrayList<>();
            for (PhaseScintillation r : groupResults) {
                batchParams.add(new Object[]{
                        r.getTs(), r.getPhi4(), r.getRawSigmaPhi(),
                        r.getSampleCount(), r.getLevelCode(), r.getLevelName(),
                        r.getCalcTime(), r.getWindowStartTime(), r.getWindowEndTime()
                });
            }

            int[] result = tdengineUtil.batchUpdate(insertSql, batchParams);
            batchResults.add(result);
        }

        // 合并结果
        return batchResults.stream()
                .flatMapToInt(Arrays::stream)
                .toArray();
    }

    @Override
    public List<PhaseScintillation> selectLatestData(int limit) {
        String sql = String.format(
                "SELECT ts, station_id, sat_no, phi4, raw_sigma_phi, sample_count, " +
                        "level_code, level_name, calc_time, window_start_time, window_end_time " +
                        "FROM %s ORDER BY ts DESC LIMIT %d",
                SUPER_TABLE_NAME, limit);

        return tdengineUtil.queryForEntityList(sql, (rs, rowNum) -> {
            PhaseScintillation entity = new PhaseScintillation();
            entity.setTs(rs.getTimestamp("ts").getTime());
            entity.setStationId(rs.getString("station_id"));
            entity.setSatNo(rs.getString("sat_no"));
            entity.setPhi4(rs.getDouble("phi4"));
            entity.setRawSigmaPhi(rs.getDouble("raw_sigma_phi"));
            entity.setSampleCount(rs.getInt("sample_count"));
            entity.setLevelCode(rs.getInt("level_code"));
            entity.setLevelName(rs.getString("level_name"));
            entity.setCalcTime(rs.getLong("calc_time"));
            entity.setWindowStartTime(rs.getLong("window_start_time"));
            entity.setWindowEndTime(rs.getLong("window_end_time"));
            return entity;
        });
    }

    @Override
    public List<PhaseScintillation> selectLatestByStation(String stationId, int limit) {
        if (StringUtils.isEmpty(stationId)) {
            return new ArrayList<>();
        }

        String sql = String.format(
                "SELECT ts, station_id, sat_no, phi4, raw_sigma_phi, sample_count, " +
                        "level_code, level_name, calc_time, window_start_time, window_end_time " +
                        "FROM %s WHERE station_id = '%s' ORDER BY ts DESC LIMIT %d",
                SUPER_TABLE_NAME, stationId, limit);

        return tdengineUtil.queryForEntityList(sql, (rs, rowNum) -> {
            PhaseScintillation entity = new PhaseScintillation();
            entity.setTs(rs.getTimestamp("ts").getTime());
            entity.setStationId(rs.getString("station_id"));
            entity.setSatNo(rs.getString("sat_no"));
            entity.setPhi4(rs.getDouble("phi4"));
            entity.setRawSigmaPhi(rs.getDouble("raw_sigma_phi"));
            entity.setSampleCount(rs.getInt("sample_count"));
            entity.setLevelCode(rs.getInt("level_code"));
            entity.setLevelName(rs.getString("level_name"));
            entity.setCalcTime(rs.getLong("calc_time"));
            entity.setWindowStartTime(rs.getLong("window_start_time"));
            entity.setWindowEndTime(rs.getLong("window_end_time"));
            return entity;
        });
    }

    @Override
    public PhaseUtil.ScintillationSummary getScintillationSummary() {
        // 查询最近5分钟内的最新数据用于汇总
        long endTime = System.currentTimeMillis();
        long startTime = endTime - 5 * 60 * 1000;

        String sql = String.format(
                "SELECT ts, station_id, sat_no, phi4, level_code, level_name " +
                        "FROM %s WHERE ts >= %d AND ts <= %d ORDER BY station_id, sat_no, ts DESC",
                SUPER_TABLE_NAME, startTime, endTime);

        List<PhaseScintillation> list = tdengineUtil.queryForEntityList(sql, (rs, rowNum) -> {
            PhaseScintillation entity = new PhaseScintillation();
            entity.setTs(rs.getTimestamp("ts").getTime());
            entity.setStationId(rs.getString("station_id"));
            entity.setSatNo(rs.getString("sat_no"));
            entity.setPhi4(rs.getDouble("phi4"));
            entity.setLevelCode(rs.getInt("level_code"));
            entity.setLevelName(rs.getString("level_name"));
            return entity;
        });

        // 去重,每个卫星取最新的一条记录
        Map<String, PhaseScintillation> latestMap = new HashMap<>();
        for (PhaseScintillation item : list) {
            String key = item.getStationId() + "_" + item.getSatNo();
            if (!latestMap.containsKey(key) || latestMap.get(key).getTs() < item.getTs()) {
                latestMap.put(key, item);
            }
        }

        // 转换为PhaseResultWithLevel格式
        List<PhaseUtil.PhaseResultWithLevel> results = new ArrayList<>();
        for (PhaseScintillation item : latestMap.values()) {
            PhaseUtil.PhaseResult base = new PhaseUtil.PhaseResult();
            base.setPhi4(item.getPhi4());
            base.setStationId(item.getStationId());
            base.setSatNo(item.getSatNo());
            PhaseUtil.PhaseResultWithLevel resultWithLevel = new PhaseUtil.PhaseResultWithLevel(base);
            results.add(resultWithLevel);
        }

        return phaseUtil.buildSummary(results);
    }

    /**
     * 转换实体对象
     */
    private PhaseScintillation convertToEntity(PhaseUtil.PhaseResultWithLevel result) {
        PhaseScintillation entity = new PhaseScintillation();
        entity.setTs(result.getCalcTime());
        entity.setStationId(result.getStationId());
        entity.setSatNo(result.getSatNo());
        entity.setPhi4(result.getPhi4());
        entity.setRawSigmaPhi(result.getRawSigmaPhi());
        entity.setSampleCount(result.getSampleCount());
        entity.setLevelCode(result.getLevel().getCode());
        entity.setLevelName(result.getLevelName());
        entity.setCalcTime(result.getCalcTime());
        entity.setWindowStartTime(result.getStartTime());
        entity.setWindowEndTime(result.getEndTime());
        return entity;
    }

    /**
     * 获取子表名称(将特殊字符替换为下划线)
     */
    private String getChildTableName(String stationId, String satNo) {
        String safeStationId = stationId.replace("-", "_").replace(".", "_");
        String safeSatNo = satNo.replace("-", "_").replace(".", "_");
        return SUPER_TABLE_NAME + "_" + safeStationId + "_" + safeSatNo;
    }

    /**
     * 通过WebSocket推送数据到大屏
     */
    private void pushToWebSocket(PhaseUtil.ScintillationSummary summary) {
        try {
            WebSocketMsg msg = new WebSocketMsg()
                    .setMsgType("data")
                    .setSendUserId(0L)
                    .setReceiveUserId(0L)
                    .setContent("相位闪烁指数数据更新")
                    .setExtraData(JSON.toJSONString(summary))
                    .setSendTime(LocalDateTime.now());

            webSocketUtil.sendToTopic(WS_TOPIC_SCINTILLATION, msg);
            log.debug("WebSocket推送相位闪烁指数数据成功, 卫星数: {}", summary.getTotalSatellites());
        } catch (Exception e) {
            log.error("WebSocket推送数据失败", e);
        }
    }

    /**
     * SQL字符串转义(防止SQL注入)
     */
    private String escapeSqlString(String str) {
        if (str == null) {
            return "";
        }
        return str.replace("'", "''");
    }
}