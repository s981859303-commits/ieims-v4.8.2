package com.ruoyi.ieims.gnss.schedule;

import com.alibaba.fastjson.JSON;
import com.ruoyi.ieims.util.GnssUtil;
import com.ruoyi.user.comm.core.redis.RedisUtil;
import com.ruoyi.user.comm.core.tdengine.TDengineUtil;
import com.ruoyi.user.comm.core.websocket.WebSocketMsg;
import com.ruoyi.user.comm.core.websocket.WebSocketUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Collectors;

/**
 * GNSS数据定时计算任务调度器
 * 负责定期计算幅度闪烁指数S4、相位闪烁指数σφ、TEC值和ROTI值
 *
 * @author guet_developer01
 * @date 2026-05-09
 */
@Component
@EnableScheduling
public class GnasSchedule {

    private static final Logger log = LoggerFactory.getLogger(GnasSchedule.class);

    /** S4计算结果Redis缓存Key前缀 */
    private static final String REDIS_KEY_S4_PREFIX = "gnss:s4:latest:";

    /** S4历史记录Redis缓存Key前缀 */
    private static final String REDIS_KEY_S4_HISTORY_PREFIX = "gnss:s4:history:";

    /** S4计算结果WebSocket推送主题 */
    private static final String WEBSOCKET_TOPIC_S4 = "/topic/gnss/s4";

    /** TDengine中S4结果超级表名 */
    private static final String TDENGINE_S4_SUPER_TABLE = "ieims.s4_result";

    /** 历史数据保留最大数量 */
    private static final int MAX_HISTORY_SIZE = 60;

    /** 分布式锁Key */
    private static final String LOCK_KEY_S4_CALC = "gnss:lock:s4:calc";

    /** 分布式锁超时时间（秒） */
    private static final long LOCK_TIMEOUT_SECONDS = 55L;

    @Autowired
    private GnssUtil gnssUtil;

    @Autowired
    private RedisUtil redisUtil;

    @Autowired
    private WebSocketUtil webSocketUtil;

    @Autowired
    private TDengineUtil tdengineUtil;

    /** 本地缓存 - 上一次计算结果（用于变化检测） */
    private final Map<String, Double> lastS4ValueMap = new ConcurrentHashMap<>();

    /** 计算锁（防止并发执行） */
    private final ReentrantLock calculateLock = new ReentrantLock();

    /** 是否启用S4计算（可通过配置文件控制） */
    @Value("${gnss.schedule.s4.enabled:true}")
    private boolean s4Enabled;

    /** 是否启用WebSocket推送 */
    @Value("${gnss.schedule.websocket.enabled:true}")
    private boolean websocketEnabled;

    /** 是否启用TDengine存储 */
    @Value("${gnss.schedule.tdengine.enabled:true}")
    private boolean tdengineEnabled;

    /**
     * 定时计算S4闪烁指数
     * 每分钟执行一次（第0秒执行）
     *
     * Cron表达式：秒 分 时 日 月 周
     * 0 * * * * ? 表示每分钟的第0秒执行
     */
    @Scheduled(cron = "0 * * * * ?")
    public void calculateS4Index() {
        // 检查是否启用
        if (!s4Enabled) {
            log.debug("S4指数计算已禁用");
            return;
        }

        // 尝试获取锁，防止集群环境下并发执行
        if (!tryAcquireDistributedLock()) {
            log.debug("获取分布式锁失败，跳过本次S4计算（可能其他节点正在执行）");
            return;
        }

        long startTime = System.currentTimeMillis();
        log.info("========== 开始执行S4闪烁指数定时计算任务 ==========");

        try {
            // 1. 确保TDengine超级表存在
            if (tdengineEnabled) {
                ensureSuperTableExists();
            }

            // 2. 调用GnssUtil计算所有站点-卫星组合的S4指数
            List<GnssUtil.S4Result> results = gnssUtil.calculateS4();

            if (results == null || results.isEmpty()) {
                log.warn("S4指数计算结果为空，可能无有效数据");
                return;
            }

            log.info("S4指数计算完成，共{}条结果，耗时{}ms",
                    results.size(), System.currentTimeMillis() - startTime);

            // 3. 获取本次计算的最大值记录
            GnssUtil.S4Result maxResult = getMaxS4Result(results);
            if (maxResult != null) {
                log.info("当前分钟S4最大值: 站点={}, 卫星={}, S4={}",
                        maxResult.getStationId(), maxResult.getSatNo(),
                        String.format("%.4f", maxResult.getS4Index()));
            }

            // 4. 存储到TDengine数据库
            if (tdengineEnabled) {
                saveS4ResultsToTDengine(results);
            }

            // 5. 处理计算结果（缓存、推送、变化检测）
            processS4Results(results);

            long endTime = System.currentTimeMillis();
            log.info("========== S4闪烁指数定时计算任务完成，总耗时{}ms ==========",
                    endTime - startTime);

        } catch (Exception e) {
            log.error("S4闪烁指数定时计算任务执行异常", e);
        } finally {
            // 释放分布式锁
            releaseDistributedLock();
        }
    }

    /**
     * 确保TDengine超级表存在
     */
    private void ensureSuperTableExists() {
        String createSuperTableSql = "CREATE STABLE IF NOT EXISTS " + TDENGINE_S4_SUPER_TABLE + " (" +
                "ts TIMESTAMP, " +
                "s4_index DOUBLE, " +
                "intensity_mean DOUBLE, " +
                "intensity_std DOUBLE, " +
                "sample_count INT, " +
                "start_time TIMESTAMP, " +
                "end_time TIMESTAMP, " +
                "scintillation_level TINYINT, " +
                "has_changed BOOL" +
                ") TAGS (station_id BINARY(64), sat_no BINARY(10))";

        try {
            tdengineUtil.executeDDL(createSuperTableSql);
            log.debug("S4结果超级表已就绪: {}", TDENGINE_S4_SUPER_TABLE);
        } catch (Exception e) {
            log.error("创建S4结果超级表失败", e);
        }
    }

    /**
     * 将S4计算结果存储到TDengine数据库
     * 注意：TDengine不能直接向超级表插入数据，需要使用子表
     *
     * @param results S4计算结果列表
     */
    private void saveS4ResultsToTDengine(List<GnssUtil.S4Result> results) {
        if (results == null || results.isEmpty()) {
            log.debug("无S4结果数据需要存储");
            return;
        }

        long startTime = System.currentTimeMillis();
        log.info("开始存储S4结果到TDengine，共{}条记录", results.size());

        int successCount = 0;
        int failCount = 0;

        for (GnssUtil.S4Result result : results) {
            if (result == null) {
                continue;
            }

            try {
                // 1. 生成子表名（使用站点ID和卫星编号组合，替换特殊字符）
                String childTableName = buildChildTableName(result.getStationId(), result.getSatNo());

                // 2. 获取旧值用于判断是否变化
                String key = buildStationSatKey(result.getStationId(), result.getSatNo());
                Double oldValue = lastS4ValueMap.get(key);
                boolean hasChanged = detectValueChange(oldValue, result.getS4Index());

                // 3. 计算闪烁等级代码
                int scintillationLevelCode = getScintillationLevelCode(result.getS4Index());

                Timestamp currentTimestamp = new Timestamp(System.currentTimeMillis());

                // 4. 使用自动建表语法插入（一条SQL完成建表和插入）
                // 语法：INSERT INTO child_table_name USING super_table TAGS (tag_value) VALUES (field_values)
                String insertSql = "INSERT INTO " + childTableName +
                        " USING " + TDENGINE_S4_SUPER_TABLE +
                        " TAGS (?, ?) " +
                        "(ts, s4_index, intensity_mean, intensity_std, sample_count, " +
                        " start_time, end_time, scintillation_level, has_changed) " +
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

                int rows = tdengineUtil.executeUpdate(insertSql,
                        result.getStationId(),                               // TAG: station_id
                        result.getSatNo(),                                   // TAG: sat_no
                        currentTimestamp,                                    // ts
                        result.getS4Index(),                                 // s4_index
                        result.getIntensityMean(),                           // intensity_mean
                        result.getIntensityStd(),                            // intensity_std
                        result.getSampleCount(),                             // sample_count
                        new Timestamp(result.getStartTime()),                // start_time
                        new Timestamp(result.getEndTime()),                  // end_time
                        scintillationLevelCode,                              // scintillation_level
                        hasChanged                                           // has_changed
                );

                if (rows > 0) {
                    successCount++;
                } else {
                    failCount++;
                    log.warn("插入S4结果返回0行: stationId={}, satNo={}",
                            result.getStationId(), result.getSatNo());
                }
            } catch (Exception e) {
                failCount++;
                log.error("存储S4结果失败: stationId={}, satNo={}",
                        result.getStationId(), result.getSatNo(), e);
            }
        }

        long endTime = System.currentTimeMillis();
        log.info("S4结果存储到TDengine完成，成功{}条，失败{}条，耗时{}ms",
                successCount, failCount, endTime - startTime);
    }

    /**
     * 构建子表名称
     * 规则：s4_ + stationId + _ + satNo，特殊字符替换为下划线
     *
     * @param stationId 站点ID
     * @param satNo 卫星编号
     * @return 子表名称
     */
    private String buildChildTableName(String stationId, String satNo) {
        // 替换特殊字符为下划线，确保表名合法
        String safeStationId = stationId.replaceAll("[^a-zA-Z0-9_]", "_");
        String safeSatNo = satNo.replaceAll("[^a-zA-Z0-9_]", "_");
        return "s4_" + safeStationId + "_" + safeSatNo;
    }

    /**
     * 检测S4值是否发生变化（超过阈值）
     *
     * @param oldValue 旧值
     * @param newValue 新值
     * @return 是否发生变化
     */
    private boolean detectValueChange(Double oldValue, double newValue) {
        if (oldValue == null) {
            return true;
        }
        double changeRate = Math.abs(newValue - oldValue) / Math.max(oldValue, 0.0001);
        return changeRate > 0.01;  // 变化超过1%视为有变化
    }

    /**
     * 根据S4值获取闪烁等级代码
     *
     * @param s4Index S4指数
     * @return 闪烁等级代码 (0-3)
     */
    private int getScintillationLevelCode(double s4Index) {
        if (s4Index < 0.2) {
            return 0;  // 正常
        } else if (s4Index < 0.4) {
            return 1;  // 中等
        } else if (s4Index < 0.6) {
            return 2;  // 较强
        } else {
            return 3;  // 强烈
        }
    }

    /**
     * 处理S4计算结果
     *
     * @param results S4计算结果列表
     */
    private void processS4Results(List<GnssUtil.S4Result> results) {
        if (results == null || results.isEmpty()) {
            return;
        }

        String currentTime = formatCurrentTime();
        List<S4PushData> pushDataList = new ArrayList<>();

        for (GnssUtil.S4Result result : results) {
            if (result == null) {
                continue;
            }

            String key = buildStationSatKey(result.getStationId(), result.getSatNo());
            Double newValue = result.getS4Index();
            Double oldValue = lastS4ValueMap.get(key);

            // 1. 缓存到Redis（最新值）
            cacheLatestS4ToRedis(result, currentTime);

            // 2. 缓存历史数据（滚动保存）
            cacheHistoryS4ToRedis(result, currentTime);

            // 3. 检测变化并准备推送数据
            boolean hasChanged = detectAndLogChange(key, oldValue, newValue, result);

            // 4. 构建推送数据
            S4PushData pushData = buildPushData(result, currentTime, hasChanged);
            pushDataList.add(pushData);

            // 5. 更新本地缓存
            lastS4ValueMap.put(key, newValue);
        }

        // 6. WebSocket推送所有计算结果
        pushToWebSocket(pushDataList);
    }

    /**
     * 缓存最新S4结果到Redis
     *
     * @param result S4计算结果
     * @param calcTime 计算时间
     */
    private void cacheLatestS4ToRedis(GnssUtil.S4Result result, String calcTime) {
        try {
            String key = REDIS_KEY_S4_PREFIX + result.getStationId() + ":" + result.getSatNo();
            Map<String, Object> dataMap = new HashMap<>();
            dataMap.put("s4Index", result.getS4Index());
            dataMap.put("intensityMean", result.getIntensityMean());
            dataMap.put("intensityStd", result.getIntensityStd());
            dataMap.put("sampleCount", result.getSampleCount());
            dataMap.put("calcTime", calcTime);
            dataMap.put("startTime", result.getStartTime());
            dataMap.put("endTime", result.getEndTime());

            // 使用setCacheMap存储（底层使用Hash结构）
            redisUtil.setCacheMap(key, dataMap);

            // 设置过期时间1小时
            redisUtil.expire(key, 3600, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("缓存S4最新结果到Redis失败: stationId={}, satNo={}",
                    result.getStationId(), result.getSatNo(), e);
        }
    }

    /**
     * 缓存S4历史数据到Redis（List结构，保留最近N条）
     * 使用 setCacheList 和 getCacheList 实现历史数据的滚动存储
     *
     * @param result S4计算结果
     * @param calcTime 计算时间
     */
    private void cacheHistoryS4ToRedis(GnssUtil.S4Result result, String calcTime) {
        try {
            String key = REDIS_KEY_S4_HISTORY_PREFIX + result.getStationId() + ":" + result.getSatNo();

            // 构建历史记录项
            Map<String, Object> historyItem = new HashMap<>();
            historyItem.put("s4Index", result.getS4Index());
            historyItem.put("calcTime", calcTime);
            historyItem.put("sampleCount", result.getSampleCount());

            String jsonValue = JSON.toJSONString(historyItem);

            // 获取现有历史列表
            List<String> historyList = redisUtil.getCacheList(key);
            if (historyList == null) {
                historyList = new ArrayList<>();
            }

            // 添加到列表头部（使用LinkedList支持头部插入）
            List<String> newHistoryList = new ArrayList<>();
            newHistoryList.add(jsonValue);
            newHistoryList.addAll(historyList);

            // 保持列表大小不超过最大限制
            if (newHistoryList.size() > MAX_HISTORY_SIZE) {
                newHistoryList = newHistoryList.subList(0, MAX_HISTORY_SIZE);
            }

            // 重新保存到Redis
            redisUtil.setCacheList(key, newHistoryList);

            // 设置过期时间（24小时）
            redisUtil.expire(key, 86400, TimeUnit.SECONDS);
        } catch (Exception e) {
            log.error("缓存S4历史数据到Redis失败: stationId={}, satNo={}",
                    result.getStationId(), result.getSatNo(), e);
        }
    }

    /**
     * 检测S4值变化并记录日志
     *
     * @param key 键（站点:卫星）
     * @param oldValue 旧值
     * @param newValue 新值
     * @param result 计算结果
     * @return 是否发生变化
     */
    private boolean detectAndLogChange(String key, Double oldValue, Double newValue,
                                       GnssUtil.S4Result result) {
        if (oldValue == null) {
            log.info("[S4首次记录] stationId={}, satNo={}, s4={}, sampleCount={}",
                    result.getStationId(), result.getSatNo(),
                    String.format("%.4f", newValue), result.getSampleCount());
            return true;
        }

        //double changeRate = Math.abs(newValue - oldValue) / Math.max(oldValue, 0.0001);
        double changeRate = Math.abs(newValue - oldValue) / 10.0;

        // 变化超过10%时记录INFO日志
        if (changeRate > 0.1) {
            log.info("[S4显著变化] stationId={}, satNo={}, oldS4={}, newS4={}, 变化率={}%",
                    result.getStationId(), result.getSatNo(),
                    String.format("%.4f", oldValue), String.format("%.4f", newValue),
                    String.format("%.2f", changeRate * 100));
            return true;
        } else if (changeRate > 0.01) {
            log.debug("[S4变化] stationId={}, satNo={}, oldS4={}, newS4={}, 变化率={}%",
                    result.getStationId(), result.getSatNo(),
                    String.format("%.4f", oldValue), String.format("%.4f", newValue),
                    String.format("%.2f", changeRate * 100));
        }

        return changeRate > 0.01;
    }

    /**
     * 构建WebSocket推送数据
     *
     * @param result S4计算结果
     * @param calcTime 计算时间
     * @param hasChanged 是否发生变化
     * @return 推送数据对象
     */
    private S4PushData buildPushData(GnssUtil.S4Result result, String calcTime, boolean hasChanged) {
        S4PushData pushData = new S4PushData();
        pushData.setStationId(result.getStationId());
        pushData.setSatNo(result.getSatNo());
        pushData.setS4Index(result.getS4Index());
        pushData.setIntensityMean(result.getIntensityMean());
        pushData.setIntensityStd(result.getIntensityStd());
        pushData.setSampleCount(result.getSampleCount());
        pushData.setCalcTime(calcTime);
        pushData.setStartTime(result.getStartTime());
        pushData.setEndTime(result.getEndTime());
        pushData.setHasChanged(hasChanged);

        // 根据S4值判断闪烁等级
        double s4 = result.getS4Index();
        if (s4 < 0.2) {
            pushData.setScintillationLevel("正常");
            pushData.setScintillationLevelCode(0);
        } else if (s4 < 0.4) {
            pushData.setScintillationLevel("中等");
            pushData.setScintillationLevelCode(1);
        } else if (s4 < 0.6) {
            pushData.setScintillationLevel("较强");
            pushData.setScintillationLevelCode(2);
        } else {
            pushData.setScintillationLevel("强烈");
            pushData.setScintillationLevelCode(3);
        }

        return pushData;
    }

    /**
     * WebSocket推送S4计算结果
     *
     * @param pushDataList 推送数据列表
     */
    private void pushToWebSocket(List<S4PushData> pushDataList) {
        if (!websocketEnabled) {
            log.debug("WebSocket推送已禁用");
            return;
        }

        if (pushDataList == null || pushDataList.isEmpty()) {
            return;
        }

        try {
            // 构建WebSocket消息
            WebSocketMsg msg = new WebSocketMsg()
                    .setMsgType("data")
                    .setSendUserId(0L)
                    .setReceiveUserId(0L)  // 0表示广播
                    .setContent("GNSS S4闪烁指数更新")
                    .setExtraData(JSON.toJSONString(pushDataList))
                    .setSendTime(LocalDateTime.now());

            // 广播到所有订阅了S4主题的用户
            webSocketUtil.sendToTopic(WEBSOCKET_TOPIC_S4, msg);

            log.debug("S4结果已推送到WebSocket，共{}条数据", pushDataList.size());
        } catch (Exception e) {
            log.error("WebSocket推送S4结果失败", e);
        }
    }

    /**
     * 构建站点-卫星组合键
     *
     * @param stationId 站点ID
     * @param satNo 卫星编号
     * @return 组合键
     */
    private String buildStationSatKey(String stationId, String satNo) {
        return stationId + ":" + satNo;
    }

    /**
     * 格式化当前时间
     *
     * @return 格式化后的时间字符串
     */
    private String formatCurrentTime() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    }

    /**
     * 尝试获取分布式锁（使用Redis）
     *
     * @return 是否获取成功
     */
    private boolean tryAcquireDistributedLock() {
        try {
            // 使用Redis的setIfAbsent实现分布式锁（支持过期时间，秒为单位）
            Boolean success = redisUtil.setIfAbsent(LOCK_KEY_S4_CALC,
                    String.valueOf(System.currentTimeMillis()),
                    LOCK_TIMEOUT_SECONDS);
            return success != null && success;
        } catch (Exception e) {
            log.warn("获取分布式锁异常，降级使用本地锁", e);
            // 降级使用本地锁
            return calculateLock.tryLock();
        }
    }

    /**
     * 释放分布式锁
     */
    private void releaseDistributedLock() {
        try {
            // 释放Redis分布式锁
            redisUtil.del(LOCK_KEY_S4_CALC);
        } catch (Exception e) {
            log.warn("释放分布式锁异常，尝试释放本地锁", e);
            // 尝试释放本地锁
            if (calculateLock.isHeldByCurrentThread()) {
                calculateLock.unlock();
            }
        }
    }

    /**
     * 获取本次计算中S4指数最大值对应的记录
     *
     * @param results S4计算结果列表
     * @return S4指数最大值对应的记录，如果结果集为空则返回null
     */
    public GnssUtil.S4Result getMaxS4Result(List<GnssUtil.S4Result> results) {
        if (results == null || results.isEmpty()) {
            log.warn("S4计算结果列表为空，无法获取最大值");
            return null;
        }

        // 使用Stream API查找S4指数最大的记录
        GnssUtil.S4Result maxS4Result = results.stream()
                .filter(Objects::nonNull)
                .max(Comparator.comparingDouble(GnssUtil.S4Result::getS4Index))
                .orElse(null);

        if (maxS4Result != null) {
            log.info("S4指数最大值: stationId={}, satNo={}, s4Index={}",
                    maxS4Result.getStationId(),
                    maxS4Result.getSatNo(),
                    String.format("%.4f", maxS4Result.getS4Index()));
        }

        return maxS4Result;
    }

    /**
     * 获取指定站点-卫星的最新S4值（对外提供查询接口）
     *
     * @param stationId 站点ID
     * @param satNo 卫星编号
     * @return S4值，未找到返回null
     */
    public Double getLatestS4Value(String stationId, String satNo) {
        if (stationId == null || satNo == null) {
            return null;
        }

        try {
            String key = REDIS_KEY_S4_PREFIX + stationId + ":" + satNo;
            Map<String, Object> dataMap = redisUtil.getCacheMap(key);
            if (dataMap != null && !dataMap.isEmpty()) {
                Object s4Obj = dataMap.get("s4Index");
                if (s4Obj instanceof Number) {
                    return ((Number) s4Obj).doubleValue();
                }
            }
        } catch (Exception e) {
            log.error("获取最新S4值失败: stationId={}, satNo={}", stationId, satNo, e);
        }
        return null;
    }

    /**
     * 获取指定站点-卫星的S4历史数据
     *
     * @param stationId 站点ID
     * @param satNo 卫星编号
     * @return 历史数据列表
     */
    public List<Map<String, Object>> getS4History(String stationId, String satNo) {
        if (stationId == null || satNo == null) {
            return Collections.emptyList();
        }

        try {
            String key = REDIS_KEY_S4_HISTORY_PREFIX + stationId + ":" + satNo;
            List<String> historyList = redisUtil.getCacheList(key);
            if (historyList == null || historyList.isEmpty()) {
                return Collections.emptyList();
            }

            List<Map<String, Object>> result = new ArrayList<>();
            for (String jsonStr : historyList) {
                @SuppressWarnings("unchecked")
                Map<String, Object> item = JSON.parseObject(jsonStr, Map.class);
                result.add(item);
            }
            return result;
        } catch (Exception e) {
            log.error("获取S4历史数据失败: stationId={}, satNo={}", stationId, satNo, e);
            return Collections.emptyList();
        }
    }

    /**
     * 从TDengine查询S4历史数据（查询子表）
     * 注意：TDengine不支持直接查询超级表的TAG字段，需要查询子表或使用超级表查询
     *
     * @param stationId 站点ID（可选，为null则查询所有）
     * @param satNo 卫星编号（可选，为null则查询所有）
     * @param startTime 开始时间
     * @param endTime 结束时间
     * @return S4历史数据列表
     */
    public List<Map<String, Object>> getS4HistoryFromTDengine(String stationId, String satNo,
                                                              long startTime, long endTime) {
        // 使用超级表查询，可以通过TAG字段过滤
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ts, s4_index, intensity_mean, intensity_std, sample_count, ")
                .append("start_time, end_time, scintillation_level, station_id, sat_no ")
                .append("FROM ").append(TDENGINE_S4_SUPER_TABLE)
                .append(" WHERE ts >= ? AND ts <= ?");

        List<Object> params = new ArrayList<>();
        params.add(new Timestamp(startTime));
        params.add(new Timestamp(endTime));

        if (stationId != null && !stationId.trim().isEmpty()) {
            sql.append(" AND station_id = ?");
            params.add(stationId);
        }

        if (satNo != null && !satNo.trim().isEmpty()) {
            sql.append(" AND sat_no = ?");
            params.add(satNo);
        }

        sql.append(" ORDER BY ts DESC");

        try {
            return tdengineUtil.queryForList(sql.toString(), params.toArray());
        } catch (Exception e) {
            log.error("从TDengine查询S4历史数据失败", e);
            return Collections.emptyList();
        }
    }

    /**
     * 手动触发S4计算（提供给Controller调用）
     *
     * @return 计算结果
     */
    public List<GnssUtil.S4Result> manualCalculateS4() {
        log.info("手动触发S4指数计算");
        return gnssUtil.calculateS4();
    }

    // ==================== 内部类 ====================

    /**
     * S4推送数据封装类
     */
    public static class S4PushData {
        private String stationId;
        private String satNo;
        private Double s4Index;
        private Double intensityMean;
        private Double intensityStd;
        private Integer sampleCount;
        private String calcTime;
        private Long startTime;
        private Long endTime;
        private Boolean hasChanged;
        private String scintillationLevel;
        private Integer scintillationLevelCode;

        // Getters and Setters
        public String getStationId() {
            return stationId;
        }

        public void setStationId(String stationId) {
            this.stationId = stationId;
        }

        public String getSatNo() {
            return satNo;
        }

        public void setSatNo(String satNo) {
            this.satNo = satNo;
        }

        public Double getS4Index() {
            return s4Index;
        }

        public void setS4Index(Double s4Index) {
            this.s4Index = s4Index;
        }

        public Double getIntensityMean() {
            return intensityMean;
        }

        public void setIntensityMean(Double intensityMean) {
            this.intensityMean = intensityMean;
        }

        public Double getIntensityStd() {
            return intensityStd;
        }

        public void setIntensityStd(Double intensityStd) {
            this.intensityStd = intensityStd;
        }

        public Integer getSampleCount() {
            return sampleCount;
        }

        public void setSampleCount(Integer sampleCount) {
            this.sampleCount = sampleCount;
        }

        public String getCalcTime() {
            return calcTime;
        }

        public void setCalcTime(String calcTime) {
            this.calcTime = calcTime;
        }

        public Long getStartTime() {
            return startTime;
        }

        public void setStartTime(Long startTime) {
            this.startTime = startTime;
        }

        public Long getEndTime() {
            return endTime;
        }

        public void setEndTime(Long endTime) {
            this.endTime = endTime;
        }

        public Boolean getHasChanged() {
            return hasChanged;
        }

        public void setHasChanged(Boolean hasChanged) {
            this.hasChanged = hasChanged;
        }

        public String getScintillationLevel() {
            return scintillationLevel;
        }

        public void setScintillationLevel(String scintillationLevel) {
            this.scintillationLevel = scintillationLevel;
        }

        public Integer getScintillationLevelCode() {
            return scintillationLevelCode;
        }

        public void setScintillationLevelCode(Integer scintillationLevelCode) {
            this.scintillationLevelCode = scintillationLevelCode;
        }
    }
    /**
     * 获取指定监测站所有卫星的最新S4数据
     *
     * @param stationId 站点ID
     * @return 卫星S4数据列表
     */
    public List<Map<String, Object>> getStationLatestS4Data(String stationId) {
        if (stationId == null || stationId.trim().isEmpty()) {
            return Collections.emptyList();
        }

        List<Map<String, Object>> result = new ArrayList<>();

        try {
            // 从Redis获取该站点所有卫星的最新数据
            String pattern = REDIS_KEY_S4_PREFIX + stationId + ":*";
            Collection<String> keys = redisUtil.keys(pattern);

            if (keys != null && !keys.isEmpty()) {
                for (String key : keys) {
                    Map<String, Object> dataMap = redisUtil.getCacheMap(key);
                    if (dataMap != null && !dataMap.isEmpty()) {
                        String satNo = key.substring(key.lastIndexOf(":") + 1);
                        Map<String, Object> satData = new HashMap<>();
                        satData.put("satNo", satNo);
                        satData.put("s4Index", dataMap.get("s4Index"));
                        satData.put("intensityMean", dataMap.get("intensityMean"));
                        satData.put("intensityStd", dataMap.get("intensityStd"));
                        satData.put("sampleCount", dataMap.get("sampleCount"));
                        satData.put("calcTime", dataMap.get("calcTime"));
                        satData.put("scintillationLevel", getScintillationLevelName((Double) dataMap.get("s4Index")));
                        satData.put("scintillationLevelCode", getScintillationLevelCode((Double) dataMap.get("s4Index")));
                        result.add(satData);
                    }
                }
            }

            // 按卫星编号排序
            result.sort((a, b) -> a.get("satNo").toString().compareTo(b.get("satNo").toString()));

        } catch (Exception e) {
            log.error("获取监测站最新S4数据失败: stationId={}", stationId, e);
        }

        return result;
    }

    /**
     * 获取所有监测站的最新S4数据概要
     *
     * @return 监测站S4数据概要列表
     */
    public List<Map<String, Object>> getAllStationsSummary() {
        List<Map<String, Object>> result = new ArrayList<>();

        try {
            // 获取所有站点Key
            String pattern = REDIS_KEY_S4_PREFIX + "*";
            Collection<String> keys = redisUtil.keys(pattern);

            if (keys == null || keys.isEmpty()) {
                return result;
            }

            // 按站点分组统计
            Map<String, List<Map<String, Object>>> stationDataMap = new HashMap<>();

            for (String key : keys) {
                Map<String, Object> dataMap = redisUtil.getCacheMap(key);
                if (dataMap != null && !dataMap.isEmpty()) {
                    String stationId = key.substring(REDIS_KEY_S4_PREFIX.length(), key.lastIndexOf(":"));
                    stationDataMap.computeIfAbsent(stationId, k -> new ArrayList<>()).add(dataMap);
                }
            }

            // 计算每个站点的统计信息
            for (Map.Entry<String, List<Map<String, Object>>> entry : stationDataMap.entrySet()) {
                String stationId = entry.getKey();
                List<Map<String, Object>> satDataList = entry.getValue();

                double maxS4 = 0.0;
                double totalS4 = 0.0;
                int warningCount = 0;  // S4 > 0.4
                int severeCount = 0;   // S4 > 0.6

                for (Map<String, Object> satData : satDataList) {
                    Double s4 = (Double) satData.get("s4Index");
                    if (s4 != null) {
                        totalS4 += s4;
                        maxS4 = Math.max(maxS4, s4);
                        if (s4 > 0.4) warningCount++;
                        if (s4 > 0.6) severeCount++;
                    }
                }

                Map<String, Object> stationSummary = new HashMap<>();
                stationSummary.put("stationId", stationId);
                stationSummary.put("satelliteCount", satDataList.size());
                stationSummary.put("avgS4", totalS4 / satDataList.size());
                stationSummary.put("maxS4", maxS4);
                stationSummary.put("warningCount", warningCount);
                stationSummary.put("severeCount", severeCount);
                stationSummary.put("status", getStationStatus(maxS4));

                result.add(stationSummary);
            }

            // 按最大S4值降序排序
            result.sort((a, b) -> Double.compare((Double) b.get("maxS4"), (Double) a.get("maxS4")));

        } catch (Exception e) {
            log.error("获取所有监测站概要失败", e);
        }

        return result;
    }

    /**
     * 获取指定卫星的S4历史趋势数据（用于ECharts）
     *
     * @param stationId 站点ID
     * @param satNo 卫星编号
     * @param hours 查询最近N小时
     * @return 趋势数据
     */
    public Map<String, Object> getSatelliteS4Trend(String stationId, String satNo, int hours) {
        Map<String, Object> result = new HashMap<>();

        try {
            long endTime = System.currentTimeMillis();
            long startTime = endTime - hours * 3600000L;

            // 从TDengine查询历史数据
            List<Map<String, Object>> historyData = getS4HistoryFromTDengine(stationId, satNo, startTime, endTime);

            List<Long> timeList = new ArrayList<>();
            List<Double> s4List = new ArrayList<>();
            List<Integer> levelList = new ArrayList<>();

            // 倒序排列，让时间从旧到新
            Collections.reverse(historyData);

            for (Map<String, Object> data : historyData) {
                Timestamp ts = (Timestamp) data.get("ts");
                Double s4 = (Double) data.get("s4_index");
                Integer level = ((Number) data.get("scintillation_level")).intValue();

                // 直接返回时间戳（毫秒）
                timeList.add(ts.getTime());
                s4List.add(s4);
                levelList.add(level);
            }

            result.put("stationId", stationId);
            result.put("satNo", satNo);
            result.put("timeList", timeList);
            result.put("s4List", s4List);
            result.put("levelList", levelList);
            result.put("currentS4", s4List.isEmpty() ? 0 : s4List.get(s4List.size() - 1));
            result.put("maxS4", s4List.stream().max(Double::compare).orElse(0.0));
            result.put("avgS4", s4List.stream().mapToDouble(Double::doubleValue).average().orElse(0.0));

        } catch (Exception e) {
            log.error("获取卫星S4趋势数据失败: stationId={}, satNo={}", stationId, satNo, e);
        }

        return result;
    }

    /**
     * 获取站点状态
     */
    private String getStationStatus(double maxS4) {
        if (maxS4 >= 0.6) {
            return "站点状态：严重";
        } else if (maxS4 >= 0.4) {
            return "站点状态：警告";
        } else if (maxS4 >= 0.2) {
            return "站点状态：注意";
        } else {
            return "站点状态：正常";
        }
    }

    /**
     * 根据S4值获取闪烁等级名称
     */
    private String getScintillationLevelName(double s4Index) {
        if (s4Index < 0.2) {
            return "正常";
        } else if (s4Index < 0.4) {
            return "中等";
        } else if (s4Index < 0.6) {
            return "较强";
        } else {
            return "强烈";
        }
    }

    /**
     * 格式化时间戳
     */
    private String formatTime(long timestamp) {
        return LocalDateTime.ofInstant(new Date(timestamp).toInstant(),
                ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern("HH:mm"));
    }
}