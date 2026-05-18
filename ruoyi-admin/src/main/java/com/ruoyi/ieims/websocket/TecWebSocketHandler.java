package com.ruoyi.ieims.websocket;

import com.ruoyi.ieims.gnss.domain.TecResult;
import com.ruoyi.user.comm.core.websocket.WebSocketMsg;
import com.ruoyi.user.comm.core.websocket.WebSocketUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.List;

/**
 * TEC WebSocket实时推送处理器
 * 集成 WebSocketUtil 通用工具
 */
@Slf4j
@Component
public class TecWebSocketHandler {

    /** 主题前缀，必须与 WebSocketConfig 中的配置一致 */
    private static final String TOPIC_BASE = "/topic/gnss/tec/";

    @Autowired
    private WebSocketUtil webSocketUtil;

    /**
     * 推送单站点的全量数据更新（用于刷新图表和表格）
     */
    public void pushStationUpdate(String stationId, List<TecResult> data) {
        try {
            WebSocketMsg msg = new WebSocketMsg();
            msg.setMsgType("data");
            msg.setContent("STATION_DATA_REFRESH"); // 告诉前端：这是全量刷新信号
            msg.setExtraData(data.toString()); // 在这里可以直接传 data，或者让前端自己再去 loadLatest
            msg.setSendTime(LocalDateTime.now());

            // 发送到：/topic/gnss/tec/guet_test2
            webSocketUtil.sendToTopic(TOPIC_BASE + stationId, msg);
            log.info("已推送站点数据更新信号: {}", stationId, data == null ? 0 : data.size());
        } catch (Exception e) {
            log.error("推送站点数据失败: {}", stationId, e);
        }
    }

    /**
     * 推送实时告警（如周跳、数据中断）
     */
    public void pushAlarm(String stationId, String warnMsg) {
        WebSocketMsg msg = new WebSocketMsg();
        msg.setMsgType("warn");
        msg.setContent(warnMsg);
        msg.setSendTime(LocalDateTime.now());

        // 发送到：/topic/gnss/tec/status
        webSocketUtil.sendToTopic(TOPIC_BASE + "status", msg);
    }
}