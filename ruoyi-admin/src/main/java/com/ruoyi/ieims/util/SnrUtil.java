package com.ruoyi.ieims.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.ruoyi.user.comm.core.websocket.WebSocketMsg;
import com.ruoyi.user.comm.core.websocket.WebSocketUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 载噪比(SNR) MQTT 实时处理工具
 */
@Slf4j
@Component
public class SnrUtil {

    @Autowired
    private WebSocketUtil webSocketUtil;

    /**
     * 处理从 MQTT 接收到的批量观测数据
     * @param payload MQTT报文内容 (JSON数组字符串)
     */
    public void processMqttPayload(String payload) {
        // 1. 记录开始时间
        long startTime = System.currentTimeMillis();
        log.info("开始处理MQTT实时SNR观测数据，报文大小: {} 字节", payload.length());

        try {
            // 1. 解析 MQTT 传来的 JSON 数组
            JSONArray jsonArray = JSON.parseArray(payload);
            if (jsonArray == null || jsonArray.isEmpty()) return;

            // 2. 构造批量转发的数据集
            List<Map<String, Object>> batchData = new ArrayList<>();

            for (int i = 0; i < jsonArray.size(); i++) {
                JSONObject obs = jsonArray.getJSONObject(i);

                String satNo = obs.getString("satNo");
                if (satNo == null || satNo.isEmpty()) continue;

                // 提取 snr 和 snr2 (若没有 snr2 或为 null，默认给 0)
                double snr = obs.getDoubleValue("snr");
                double snr2 = obs.containsKey("snr2") ? obs.getDoubleValue("snr2") : 0.0;

                // 四舍五入保留两位小数，减小传输体积
                Map<String, Object> data = new HashMap<>(3);
                data.put("satNo", satNo);
                data.put("snr", Math.round(snr * 100) / 100.0);
                data.put("snr2", Math.round(snr2 * 100) / 100.0);

                batchData.add(data);
            }

            // 3. 将这批卫星数据一次性打包发送给前端
            if (!batchData.isEmpty()) {
                WebSocketMsg msg = new WebSocketMsg()
                        .setMsgType("snr_batch") // 注意：类型改为 snr_batch
                        .setSendUserId(0L)
                        .setReceiveUserId(0L)
                        .setExtraData(JSON.toJSONString(batchData)) // 塞入整个数组
                        .setSendTime(LocalDateTime.now());

                webSocketUtil.sendToTopic("/topic/snr", msg);
            }
            // 2. 记录结束时间并计算耗时
            long costTime = System.currentTimeMillis() - startTime;
            log.info("MQTT实时SNR数据处理并推送完成，成功解析 {} 条，耗时 {}ms", batchData.size(), costTime);
        } catch (Exception e) {
            // 异常时也记录耗时，方便排查由于脏数据导致的解析卡顿
            long costTime = System.currentTimeMillis() - startTime;
            log.error("处理MQTT实时SNR观测数据异常，耗时 {}ms", costTime, e);
        }
    }
}