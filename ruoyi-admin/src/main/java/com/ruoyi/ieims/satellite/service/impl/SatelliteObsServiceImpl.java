package com.ruoyi.ieims.satellite.service.impl;

import com.ruoyi.ieims.satellite.domain.SatelliteObsData;
import com.ruoyi.ieims.satellite.service.ISatelliteObsService;
import com.ruoyi.user.comm.core.tdengine.TDengineUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * 卫星观测数据 服务层实现
 *
 * @author guet_developer01
 * @date 2026-04-30
 */
@Slf4j
@Service
public class SatelliteObsServiceImpl implements ISatelliteObsService {

    @Autowired
    private TDengineUtil tdengineUtil;

    @Override
    public void batchSaveToTDengine(List<SatelliteObsData> dataList) {
        if (dataList == null || dataList.isEmpty()) {
            return;
        }

        // 按 站点ID + 卫星编号 分组，构建独立的子表名称避免 TDengine 时间戳冲突
        Map<String, List<SatelliteObsData>> groupedData = dataList.stream()
                .collect(Collectors.groupingBy(data ->
                        "tb_sat_obs_" + data.getStationId() + "_" + data.getSatNo()
                ));

        for (Map.Entry<String, List<SatelliteObsData>> entry : groupedData.entrySet()) {
            String tableName = entry.getKey();
            List<SatelliteObsData> subDataList = entry.getValue();

            // 获取子表的 Tags（同一子表的 Tag 是相同的，取第一条即可）
            SatelliteObsData firstData = subDataList.get(0);
            String stationId = firstData.getStationId();
            String satNo = firstData.getSatNo();
            String satSystem = firstData.getSatSystem();

            // 构建带有自动建表语法的 SQL
            String sql = String.format(
                    "INSERT INTO ieims.%s USING ieims.st_sat_obs TAGS ('%s', '%s', '%s') " +
                            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    tableName, stationId, satNo, satSystem
            );

            List<Object[]> batchParams = new ArrayList<>();
            for (SatelliteObsData data : subDataList) {
                Object[] params = new Object[]{
                        new Timestamp(data.getEpochTime()), // ts主键
                        data.getObsUniqueKey(),
                        data.getDataSource(),
                        data.getComplete(),
                        data.getTimestamp(),
                        data.getDateSource(),
                        data.getDateFromZda(),
                        data.getObservationTime(),
                        data.getElevation(),
                        data.getAzimuth(),
                        data.getSnr(),
                        data.getPseudorangeP1(),
                        data.getPseudorangeP2(),
                        data.getPhaseL1(),
                        data.getPhaseP2(),
                        data.getC1(),
                        data.getC2()
                };
                batchParams.add(params);
            }

            try {
                tdengineUtil.batchUpdate(sql, batchParams);
//                log.debug("成功写入卫星数据至子表: {}, 条数: {}", tableName, batchParams.size());
            } catch (Exception e) {
                log.error("写入TDengine失败，表名: {}", tableName, e);
            }
        }
    }
}