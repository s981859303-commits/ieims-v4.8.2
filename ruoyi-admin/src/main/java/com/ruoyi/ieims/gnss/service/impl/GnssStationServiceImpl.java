package com.ruoyi.ieims.gnss.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import com.ruoyi.ieims.gnss.mapper.GnssStationMapper;
import com.ruoyi.ieims.gnss.domain.GnssStation;
import com.ruoyi.ieims.gnss.domain.GnssSolutionData;
import com.ruoyi.ieims.gnss.service.IGnssStationService;
import com.ruoyi.common.utils.DateUtils;
import com.ruoyi.user.comm.core.redis.RedisUtil;
import com.ruoyi.user.comm.core.tdengine.TDengineUtil;
import com.ruoyi.user.comm.core.websocket.WebSocketUtil;
import com.ruoyi.user.comm.core.websocket.WebSocketMsg;
import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import javax.annotation.Resource;

/**
 * GNSS监测站设备信息Service业务层处理
 *
 * @author guet_developer01
 * @date 2026-04-26
 */
@Service
@Slf4j
public class GnssStationServiceImpl implements IGnssStationService
{
    @Resource
    private GnssStationMapper gnssStationMapper;

    @Autowired
    private TDengineUtil tdengineUtil;

    @Autowired
    private RedisUtil redisUtil;

    @Autowired
    private WebSocketUtil webSocketUtil;

    // 批量缓存队列（线程安全）
    private final ConcurrentLinkedQueue<GnssSolutionData> dataQueue = new ConcurrentLinkedQueue<>();

    // 批量写入阈值
    private static final int BATCH_SIZE = 100;

    // 批量写入时间间隔（毫秒）
    private static final long BATCH_INTERVAL = 100;

    /**
     * 初始化批量写入任务
     */
    public GnssStationServiceImpl() {
        ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();
        executor.scheduleAtFixedRate(this::flushBatchData, BATCH_INTERVAL, BATCH_INTERVAL, TimeUnit.MILLISECONDS);
    }

    /**
     * 查询GNSS监测站设备信息
     *
     * @param id GNSS监测站设备信息主键
     * @return GNSS监测站设备信息
     */
    @Override
    public GnssStation selectGnssStationById(Long id)
    {
        return gnssStationMapper.selectGnssStationById(id);
    }

    /**
     * 根据站点ID查询监测站信息
     *
     * @param stationId 站点ID
     * @return GNSS监测站设备信息
     */
    @Override
    public GnssStation selectGnssStationByStationId(String stationId) {
        return gnssStationMapper.selectGnssStationByStationId(stationId);
    }

    /**
     * 查询GNSS监测站设备信息列表
     *
     * @param gnssStation GNSS监测站设备信息
     * @return GNSS监测站设备信息集合
     */
    @Override
    public List<GnssStation> selectGnssStationList(GnssStation gnssStation)
    {
        return gnssStationMapper.selectGnssStationList(gnssStation);
    }

    /**
     * 新增GNSS监测站设备信息
     *
     * @param gnssStation GNSS监测站设备信息
     * @return 结果
     */
    @Override
    public int insertGnssStation(GnssStation gnssStation)
    {
        gnssStation.setCreateTime(DateUtils.getNowDate());
        gnssStation.setUpdateTime(DateUtils.getNowDate());
        return gnssStationMapper.insertGnssStation(gnssStation);
    }

    /**
     * 修改GNSS监测站设备信息
     *
     * @param gnssStation GNSS监测站设备信息
     * @return 结果
     */
    @Override
    public int updateGnssStation(GnssStation gnssStation)
    {
        gnssStation.setUpdateTime(DateUtils.getNowDate());
        return gnssStationMapper.updateGnssStation(gnssStation);
    }

    /**
     * 更新监测站在线状态
     *
     * @param stationId 站点ID
     * @param onlineStatus 在线状态
     * @return 结果
     */
    @Override
    public int updateStationOnlineStatus(String stationId, String onlineStatus) {
        return gnssStationMapper.updateStationOnlineStatus(stationId, onlineStatus, new Date());
    }

    /**
     * 批量删除GNSS监测站设备信息
     *
     * @param ids 需要删除的主键集合
     * @return 结果
     */
    @Override
    public int deleteGnssStationByIds(Long[] ids)
    {
        return gnssStationMapper.deleteGnssStationByIds(ids);
    }

    /**
     * 删除GNSS监测站设备信息信息
     *
     * @param id GNSS监测站设备信息主键
     * @return 结果
     */
    @Override
    public int deleteGnssStationById(Long id)
    {
        return gnssStationMapper.deleteGnssStationById(id);
    }

    /**
     * 处理GNSS解算数据并写入TDengine
     *
     * @param data GNSS解算数据
     */
    @Override
    public void handleGnssSolutionData(GnssSolutionData data) {
        try {
            // 1. 添加到批量队列
            dataQueue.offer(data);

            // 2. 更新Redis缓存（最新数据，有效期5分钟）
            String redisKey = "gnss:latest:" + data.getStationId();
            redisUtil.set(redisKey, JSON.toJSONString(data), 300, TimeUnit.SECONDS);

            // 3. 更新设备在线状态
            updateStationOnlineStatus(data.getStationId(), "1");

            // 4. WebSocket推送实时数据
            WebSocketMsg msg = new WebSocketMsg()
                    .setMsgType("data")
                    .setSendUserId(0L)
                    .setContent("GNSS数据更新")
                    .setExtraData(JSON.toJSONString(data));
            webSocketUtil.sendToTopic("/topic/gnss/solution", msg);

            // 5. 如果队列达到阈值，立即触发批量写入
            if (dataQueue.size() >= BATCH_SIZE) {
                flushBatchData();
            }
        } catch (Exception e) {
            log.error("处理GNSS解算数据失败", e);
        }
    }

    /**
     * 批量写入GNSS数据到TDengine
     *
     * @param dataList 数据列表
     */
    @Override
    public void batchInsertGnssDataToTDengine(List<GnssSolutionData> dataList) {
        if (dataList == null || dataList.isEmpty()) {
            return;
        }

        // 按站点分组，批量写入
        Map<String, List<GnssSolutionData>> groupByStation = dataList.stream()
                .collect(java.util.stream.Collectors.groupingBy(GnssSolutionData::getStationId));

        for (Map.Entry<String, List<GnssSolutionData>> entry : groupByStation.entrySet()) {
            String stationId = entry.getKey();
            List<GnssSolutionData> stationDataList = entry.getValue();

            // 构建批量插入参数
            String sql = String.format(
                    "INSERT INTO gnss_solution_%s USING gnss_solution_data TAGS ('%s', '%s') (ts, latitude, longitude, altitude, status, solution_type, satellite_count, hdop, valid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    stationId, stationId, stationId
            );

            List<Object[]> batchParams = new ArrayList<>();
            for (GnssSolutionData data : stationDataList) {
                // 转换时间戳（毫秒）为TDengine的TIMESTAMP
                long ts = data.getTime();
                Object[] params = new Object[]{
                        new java.sql.Timestamp(ts),
                        data.getLatitude(),
                        data.getLongitude(),
                        data.getAltitude(),
                        data.getStatus(),
                        data.getSolutionType(),
                        data.getSatelliteCount(),
                        data.getHdop(),
                        data.getValid()
                };
                batchParams.add(params);
            }

            // 批量写入TDengine，失败重试3次
            int retryCount = 0;
            while (retryCount < 3) {
                try {
                    tdengineUtil.batchUpdate(sql, batchParams);
//                    log.info("批量写入TDengine成功，站点：{}，数据条数：{}", stationId, batchParams.size());
                    break;
                } catch (Exception e) {
                    retryCount++;
                    log.error("第{}次写入TDengine失败，站点：{}，错误信息：{}", retryCount, stationId, e.getMessage());
                    if (retryCount >= 3) {
                        log.error("批量写入TDengine最终失败，站点：{}，数据条数：{}", stationId, batchParams.size(), e);
                        // 可将失败数据写入本地文件或消息队列，后续重试
                    }
                    try {
                        TimeUnit.MILLISECONDS.sleep(100 * retryCount);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
    }

    /**
     * 刷新批量数据到TDengine
     */
    private void flushBatchData() {
        List<GnssSolutionData> batchList = new ArrayList<>();
        GnssSolutionData data;
        while ((data = dataQueue.poll()) != null) {
            batchList.add(data);
            // 防止单次处理过多数据
            if (batchList.size() >= BATCH_SIZE) {
                batchInsertGnssDataToTDengine(batchList);
                batchList.clear();
            }
        }
        if (!batchList.isEmpty()) {
            batchInsertGnssDataToTDengine(batchList);
        }
    }
}