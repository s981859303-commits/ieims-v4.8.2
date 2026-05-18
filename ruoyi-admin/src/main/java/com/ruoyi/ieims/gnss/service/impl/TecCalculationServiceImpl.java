package com.ruoyi.ieims.gnss.service.impl;

import com.ruoyi.ieims.gnss.domain.TecCalculationArc;
import com.ruoyi.ieims.gnss.domain.TecResult;
import com.ruoyi.ieims.gnss.mapper.TecResultMapper;
import com.ruoyi.ieims.gnss.service.ITecCalculationService;
import com.ruoyi.ieims.util.ArcManager;
import com.ruoyi.ieims.util.TecCalculator;
import com.ruoyi.ieims.websocket.TecWebSocketHandler;
import com.ruoyi.user.comm.core.tdengine.TDengineUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TEC计算服务实现
 *
 * @author guet_developer01
 * @date 2026-05-12
 */
@Slf4j
@Service
public class TecCalculationServiceImpl implements ITecCalculationService {

    @Autowired
    private ArcManager arcManager;
    @Autowired
    private TecCalculator calculator;
    @Autowired
    private TecResultMapper tecResultMapper;
    @Autowired
    private TDengineUtil tdengineUtil;
    @Autowired
    private TecWebSocketHandler tecWebSocketHandler;

    @Override
    public List<TecResult> selectTecResultList(String stationId, String satNo,
                                               java.util.Date beginTime, java.util.Date endTime) {
        return tecResultMapper.selectTecResultList(stationId, satNo, beginTime, endTime);
    }

    @Override
    public void computeAndStoreTec() {
        Map<String, TecCalculationArc> readyArcs = arcManager.getReadyArcsForCalculation();

        if (readyArcs.isEmpty()) {
            log.info("本次定时任务无就绪弧段");
            return;
        }

        List<TecResult> results = new ArrayList<>();

        // 遍历 Map.Entry，传入 key 给 calculate
        for (Map.Entry<String, TecCalculationArc> entry : readyArcs.entrySet()) {
            String key = entry.getKey();
            TecCalculationArc arc = entry.getValue();
            try {
                // 传入两个参数 (arc, key)
                TecResult r = calculator.calculate(arc, key);
                if (r != null) {
                    results.add(r);
                }
            } catch (Exception e) {
                log.error("TEC计算异常, key={}", key, e);
            } finally {
                // 计算后移除弧段，防止下次重复结算
                arcManager.removeArc(key);
            }
        }

        if (results.isEmpty()) {
            log.debug("本次无有效TEC结果可入库");
            return;
        }

        batchInsertToTDengine(results);
        notifyFrontend(results);
    }

    /**
     * 批量写入TDengine，使用USING语法自动创建子表
     * 子表命名规则：tec_result_data_{stationId}_{satNo}
     */
    private void batchInsertToTDengine(List<TecResult> results) {
        // 按子表分组（同一站点+同一卫星的数据写入同一子表）
        Map<String, List<TecResult>> groupBySubTable = new HashMap<>();
        for (TecResult r : results) {
            String subTableName = generateSubTableName(r.getStationId(), r.getSatNo());
            groupBySubTable.computeIfAbsent(subTableName, k -> new ArrayList<>()).add(r);
        }

        for (Map.Entry<String, List<TecResult>> entry : groupBySubTable.entrySet()) {
            String subTableName = entry.getKey();
            List<TecResult> group = entry.getValue();
            if (group.isEmpty()) {
                continue;
            }

            TecResult first = group.get(0);
            // USING语法：自动创建子表（如果不存在）
            String sql = "INSERT INTO ieims." + subTableName
                    + " USING ieims.tec_result_data TAGS (?, ?, ?)"
                    + " (ts, stec, vtec, dcb_offset, quality_flag, valid_epochs, elev_span, dcb_corrected, slip_count, mapping_func, ipp_lat, ipp_lon)"
                    + " VALUES ";

            StringBuilder sb = new StringBuilder(sql);
            List<Object> params = new ArrayList<>();
            // TAGS值
            params.add(first.getStationId());
            params.add(first.getSatNo());
            params.add(first.getSatSystem());

            for (int i = 0; i < group.size(); i++) {
                TecResult r = group.get(i);
                if (i > 0) {
                    sb.append(", ");
                }
                sb.append("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
                params.add(r.getTs());
                params.add(r.getStec());
                params.add(r.getVtec());
                params.add(r.getDcbEstimate());
                params.add(r.getQualityFlag());
                params.add(r.getValidEpochCount());
                params.add(r.getElevSpan());
                params.add(r.getDcbCorrected());
                params.add(r.getSlipCount());
                params.add(r.getMappingFunc());
                params.add(r.getIppLat());
                params.add(r.getIppLon());
            }

            try {
                // 使用executeUpdate执行拼接后的SQL
                tdengineUtil.executeUpdate(sb.toString(), params.toArray());
                log.info("TEC结果写入TDengine成功, 子表={}, 数量={}", subTableName, group.size());
            } catch (Exception e) {
                log.error("TEC结果写入TDengine失败, 子表={}, 数量={}", subTableName, group.size(), e);
            }
        }
    }

    /**
     * 生成子表名（去除特殊字符，避免SQL注入）
     */
    private String generateSubTableName(String stationId, String satNo) {
        String safeStation = stationId.replaceAll("[^a-zA-Z0-9_]", "_");
        String safeSatNo = satNo.replaceAll("[^a-zA-Z0-9_]", "_");
        return "tec_result_data_" + safeStation + "_" + safeSatNo;
    }

    private void notifyFrontend(List<TecResult> results) {
        if (results == null || results.isEmpty()) return;

        try {
            // 因为我们的 WebSocket 现在是按站点订阅的（/topic/gnss/tec/{stationId}）
            // 所以我们需要取出这次计算结果属于哪个站点，然后触发该站点的刷新信号
            String stationId = results.get(0).getStationId();

            // 调用新版 Handler 中的方法，发送刷新信号
            tecWebSocketHandler.pushStationUpdate(stationId, results);

            log.info("已触发站点 {} 的 WebSocket 前端刷新信号", stationId);
        } catch (Exception e) {
            log.error("WebSocket推送TEC结果失败", e);
        }
    }

    @Override
    public List<TecResult> getLatestTecByStation(String stationId, int limit) {
        return tecResultMapper.selectLatestByStation(stationId, limit);
    }
}