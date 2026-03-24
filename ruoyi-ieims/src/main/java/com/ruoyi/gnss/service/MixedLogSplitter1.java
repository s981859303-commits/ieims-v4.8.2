package com.ruoyi.gnss.service;

import cn.hutool.json.JSONArray;
import com.ruoyi.common.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 混合日志解析器
 *
 * 功能：
 * 1. 解析 NMEA 和 RTCM 混合数据流
 * 2. 调用 RtklibNative 解析 RTCM 观测数据
 * 3. 将数据通过 MQTT 发送到 TDengine
 *
 * 优化内容：
 * 1. 使用 ReentrantLock 保证线程安全
 * 2. 使用 SLF4J 日志框架
 * 3. 配置参数外部化
 * 4. 清理冗余代码
 */
@Service
public class MixedLogSplitter1 {

    private static final Logger logger = LoggerFactory.getLogger(MixedLogSplitter1.class);

    // --- 配置参数 ---
    @Value("${gnss.parser.bufferSize:1048576}")
    private int bufferSize;

    @Value("${gnss.parser.stationId:8900_1}")
    private String stationId;

    @Value("${gnss.parser.maxNmeaLength:1000}")
    private int maxNmeaLength;

    @Value("${gnss.parser.maxRtcmLength:1200}")
    private int maxRtcmLength;

    @Value("${gnss.parser.printIntervalMs:950}")
    private long printIntervalMs;

    // --- 依赖注入 ---
    @Autowired(required = false)
    private List<GnssDataListener> listeners = new ArrayList<>();

    @Autowired(required = false)
    private GnssMqttService mqttService;

    // --- 缓冲区和锁 ---
    private ByteBuffer buffer;
    private final ReentrantLock bufferLock = new ReentrantLock();

    // --- MQTT 主题配置 ---
    @Value("${gnss.mqtt.tdengineTopic:tdengine/rtcm/data}")
    private String tdengineMqttTopic;

    // --- 卫星数据缓存 ---
    private final Map<Integer, SatData> satCache = new HashMap<>();
    private long lastPrintTime = System.currentTimeMillis();

    // --- 统计计数器 ---
    private volatile long nmeaCount = 0;
    private volatile long rtcmCount = 0;

    /**
     * 初始化方法
     */
    @javax.annotation.PostConstruct
    public void init() {
        this.buffer = ByteBuffer.allocate(bufferSize);
        logger.info("MixedLogSplitter 初始化完成");
        logger.info("  - 缓冲区大小: {} 字节", bufferSize);
        logger.info("  - 站点ID: {}", stationId);
    }

    /**
     * 入口方法：接收原始字节流
     */
    public void pushData(byte[] newBytes) {
        if (newBytes == null || newBytes.length == 0) {
            return;
        }

        logger.debug("收到数据: {} 字节", newBytes.length);

        bufferLock.lock();
        try {
            if (buffer.remaining() < newBytes.length) {
                buffer.compact();
                if (buffer.remaining() < newBytes.length) {
                    logger.warn("缓冲区空间不足，清空缓冲区");
                    buffer.clear();
                }
            }

            buffer.put(newBytes);
            buffer.flip();
            parseAndDispatch();
            buffer.compact();

        } catch (Exception e) {
            logger.error("数据解析异常: {}", e.getMessage(), e);
            recoverBufferState();
        } finally {
            bufferLock.unlock();
        }
    }

    /**
     * 恢复 buffer 状态
     */
    private void recoverBufferState() {
        try {
            if (buffer != null) {
                buffer.clear();
                logger.info("缓冲区已重置");
            }
        } catch (Exception e) {
            logger.error("恢复缓冲区状态失败: {}", e.getMessage());
        }
    }

    /**
     * 解析并分发数据
     */
    private void parseAndDispatch() {
        while (buffer.hasRemaining()) {
            buffer.mark();
            byte b = buffer.get();

            if (b == 0x24) { // NMEA ($)
                if (!tryExtractNmea()) {
                    buffer.reset();
                    break;
                }
            } else if ((b & 0xFF) == 0xD3) { // RTCM (0xD3)
                if (!tryExtractRtcm()) {
                    buffer.reset();
                    break;
                }
            }
        }
    }

    /**
     * 提取 NMEA 语句
     */
    private boolean tryExtractNmea() {
        int startPos = buffer.position() - 1;

        for (int i = buffer.position(); i < buffer.limit(); i++) {
            if (i - startPos > maxNmeaLength) {
                buffer.position(i);
                logger.warn("NMEA 语句过长，已跳过");
                return true;
            }

            if (buffer.get(i) == 0x0A) {
                int len = i - startPos + 1;
                byte[] lineBytes = new byte[len];
                buffer.position(startPos);
                buffer.get(lineBytes);

                String nmea = new String(lineBytes, StandardCharsets.US_ASCII).trim();
                notifyNmea(nmea);

                buffer.position(i + 1);
                return true;
            }
        }
        return false;
    }

    /**
     * 提取 RTCM 帧
     */
    private boolean tryExtractRtcm() {
        if (buffer.remaining() < 2) {
            return false;
        }

        int p = buffer.position();
        int length = ((buffer.get(p) & 0x03) << 8) | (buffer.get(p + 1) & 0xFF);

        if (length > maxRtcmLength || length == 0) {
            buffer.reset();
            buffer.get();
            logger.debug("检测到假 RTCM 头，已跳过");
            return true;
        }

        int totalLen = length + 6;

        if (buffer.remaining() < totalLen - 1) {
            return false;
        }

        byte[] rtcmFrame = new byte[totalLen];
        buffer.position(buffer.position() - 1);
        buffer.get(rtcmFrame);

        notifyRtcm(rtcmFrame);
        return true;
    }

    /**
     * 通知 NMEA 数据
     */
    private void notifyNmea(String nmea) {
        nmeaCount++;

        // 分发给监听器
        if (listeners != null && !listeners.isEmpty()) {
            for (GnssDataListener listener : listeners) {
                try {
                    listener.onNmeaReceived(nmea);
                } catch (Exception e) {
                    logger.error("监听器处理 NMEA 异常: {}", e.getMessage());
                }
            }
        }

        // 解析 GGA 数据并推送到 TDengine
        if (nmea.startsWith("$GPGGA") || nmea.startsWith("$GNGGA") || nmea.startsWith("$BDGGA")) {
            parseAndSendGgaToTDengine(nmea);
        }
    }

    /**
     * 解析 GGA 数据并发送到 TDengine
     */
    private void parseAndSendGgaToTDengine(String ggaLine) {
        try {
            String[] parts = ggaLine.split(",", -1);

            if (parts.length < 10 || parts[2].isEmpty() || parts[4].isEmpty()) {
                return;
            }

            double lat = nmeaToDecimal(parts[2], parts[3]);
            double lon = nmeaToDecimal(parts[4], parts[5]);
            int satUsed = parts[7].isEmpty() ? 0 : Integer.parseInt(parts[7]);
            double hdop = parts[8].isEmpty() ? 0.0 : Double.parseDouble(parts[8]);
            double alt = parts[9].isEmpty() ? 0.0 : Double.parseDouble(parts[9]);

            // 组装 JSON
            JSONObject metric = new JSONObject();
            metric.put("name", "st_receiver_status");
            metric.put("timestamp", System.currentTimeMillis());

            JSONObject tags = new JSONObject();
            tags.put("station_id", stationId);
            metric.put("tags", tags);

            JSONObject fields = new JSONObject();
            fields.put("lat", lat);
            fields.put("lon", lon);
            fields.put("alt", alt);
            fields.put("sat_used", satUsed);
            fields.put("hdop", hdop);
            metric.put("fields", fields);

            JSONArray metricsArray = new JSONArray();
            metricsArray.add(metric);

            JSONObject finalPayload = new JSONObject();
            finalPayload.put("metrics", metricsArray);

            // 发送给 MQTT
            if (mqttService != null && mqttService.isConnected()) {
                mqttService.publish(tdengineMqttTopic, finalPayload.toString());
                logger.debug("GGA 数据已推送: Lat={}, Lon={}", lat, lon);
            }

        } catch (Exception e) {
            logger.error("GGA 解析失败: {}", e.getMessage());
        }
    }

    /**
     * NMEA 经纬度转换为十进制度
     */
    private double nmeaToDecimal(String nmeaPos, String dir) {
        if (nmeaPos == null || nmeaPos.isEmpty()) {
            return 0.0;
        }

        int dotIndex = nmeaPos.indexOf(".");
        if (dotIndex < 2) {
            return 0.0;
        }

        try {
            int deg = Integer.parseInt(nmeaPos.substring(0, dotIndex - 2));
            double min = Double.parseDouble(nmeaPos.substring(dotIndex - 2));
            double decimal = deg + (min / 60.0);

            if ("S".equalsIgnoreCase(dir) || "W".equalsIgnoreCase(dir)) {
                decimal = -decimal;
            }
            return decimal;
        } catch (NumberFormatException e) {
            logger.warn("经纬度解析失败: {} {}", nmeaPos, dir);
            return 0.0;
        }
    }

    /**
     * 通知 RTCM 数据
     */
    private void notifyRtcm(byte[] data) {
        rtcmCount++;
        logger.debug("识别到 RTCM 帧，长度: {} 字节", data.length);

        // 调用 RtklibNative 解析 RTCM
        decodeRtcmData(data);

        // 分发给监听器
        if (listeners != null && !listeners.isEmpty()) {
            for (GnssDataListener listener : listeners) {
                try {
                    listener.onRtcmReceived(data);
                } catch (Exception e) {
                    logger.error("监听器处理 RTCM 异常: {}", e.getMessage());
                }
            }
        }
    }

    /**
     * 调用 RtklibNative 解析 RTCM 数据
     */
    private void decodeRtcmData(byte[] rtcmData) {
        try {
            // 准备内存空间：一次最多接收 64 颗卫星的数据
            RtklibNative.JavaObs.ByReference obsRef = new RtklibNative.JavaObs.ByReference();
            RtklibNative.JavaObs[] obsArray = (RtklibNative.JavaObs[]) obsRef.toArray(64);

            // 调用 C 语言 DLL 函数
            int count = RtklibNative.INSTANCE.parse_rtcm_frame(rtcmData, rtcmData.length, obsRef, 64);

            // 如果 count > 0，说明是观测值帧
            if (count > 0) {
                updateSatCache(obsArray, count);
            }
        } catch (UnsatisfiedLinkError e) {
            logger.error("找不到 rtklib_bridge.dll 文件！请确保它在项目根目录下。");
        } catch (Exception e) {
            logger.error("RTCM 解码异常: {}", e.getMessage());
        }
    }

    /**
     * 更新卫星数据缓存
     */
    private void updateSatCache(RtklibNative.JavaObs[] newObs, int count) {
        for (int i = 0; i < count; i++) {
            RtklibNative.JavaObs obs = newObs[i];

            // 过滤无效数据
            if (obs.P[0] == 0.0) {
                continue;
            }

            SatData data = new SatData();
            data.sat = obs.sat;
            data.satName = new String(obs.id).trim();

            // 解析信号代码
            if (obs.code != null && obs.code.length >= 8) {
                data.c1 = new String(obs.code, 0, 8).trim();
            }
            if (obs.code != null && obs.code.length >= 16) {
                data.c2 = new String(obs.code, 8, 8).trim();
            }

            data.p1 = obs.P[0];
            data.l1 = obs.L[0];
            data.p2 = (obs.P[1] != 0.0) ? obs.P[1] : null;
            data.l2 = (obs.L[1] != 0.0) ? obs.L[1] : null;
            data.s1 = (obs.snr[0] > 0) ? obs.snr[0] * 4.0 : 0;

            satCache.put(data.sat, data);
        }

        // 定时打印和推送
        long now = System.currentTimeMillis();
        if (now - lastPrintTime > printIntervalMs) {
            printAndPublishCache();
            lastPrintTime = now;
        }
    }

    /**
     * 打印缓存数据并推送到 MQTT
     */
    private void printAndPublishCache() {
        if (satCache.isEmpty()) {
            return;
        }

        // 打印到控制台
        StringBuilder sb = new StringBuilder();
        sb.append("=======================================================================================================\n");
        sb.append(String.format("RTCM 实时数据 | 时间: %s | 总星数: %d\n", new java.util.Date(), satCache.size()));
        sb.append(String.format("%-6s %-4s %-4s %-8s %-14s %-14s %-14s %-14s\n",
                "SAT", "C1", "C2", "S1(dBHz)", "P1(m)", "P2(m)", "L1(cycle)", "L2(cycle)"));
        sb.append("-------------------------------------------------------------------------------------------------------\n");

        JSONArray metricsArray = new JSONArray();
        long currentTimestamp = System.currentTimeMillis();

        satCache.values().stream()
                .sorted(Comparator.comparingInt(a -> a.sat))
                .forEach(d -> {
                    sb.append(String.format("%-6s %-4s %-4s %-8s %-14s %-14s %-14s %-14s\n",
                            d.satName,
                            d.c1 != null ? d.c1 : "",
                            d.c2 != null ? d.c2 : "",
                            formatVal(d.s1), formatVal(d.p1), formatVal(d.p2),
                            formatVal(d.l1), formatVal(d.l2)));

                    // 构建 JSON 数据点
                    JSONObject metric = new JSONObject();
                    metric.put("name", "st_satellite_obs");
                    metric.put("timestamp", currentTimestamp);

                    JSONObject tags = new JSONObject();
                    tags.put("station_id", stationId);
                    tags.put("sat_name", d.satName);
                    metric.put("tags", tags);

                    JSONObject fields = new JSONObject();
                    if (d.c1 != null && !d.c1.isEmpty()) fields.put("c1", d.c1);
                    if (d.c2 != null && !d.c2.isEmpty()) fields.put("c2", d.c2);
                    if (d.s1 != null) fields.put("s1", d.s1);
                    if (d.p1 != null) fields.put("p1", d.p1);
                    if (d.p2 != null) fields.put("p2", d.p2);
                    if (d.l1 != null) fields.put("l1", d.l1);
                    if (d.l2 != null) fields.put("l2", d.l2);
                    metric.put("fields", fields);

                    metricsArray.add(metric);
                });

        sb.append("=======================================================================================================\n");
        logger.info(sb.toString());

        // 发送到 MQTT
        try {
            if (!metricsArray.isEmpty() && mqttService != null && mqttService.isConnected()) {
                JSONObject finalPayload = new JSONObject();
                finalPayload.put("metrics", metricsArray);
                mqttService.publish(tdengineMqttTopic, finalPayload.toString());
            }
        } catch (Exception e) {
            logger.error("发送 MQTT 数据失败: {}", e.getMessage());
        }

        satCache.clear();
    }

    /**
     * 格式化数值
     */
    private String formatVal(Double val) {
        if (val == null || val == 0.0) {
            return "null";
        }
        return String.format(java.util.Locale.US, "%.3f", val);
    }

    /**
     * 获取统计信息
     */
    public String getStatistics() {
        return String.format("NMEA: %d 条, RTCM: %d 帧", nmeaCount, rtcmCount);
    }

    /**
     * 重置统计计数器
     */
    public void resetStatistics() {
        nmeaCount = 0;
        rtcmCount = 0;
    }

    /**
     * 卫星数据内部类
     */
    private static class SatData {
        int sat;
        Double p1, p2, l1, l2, s1;
        String satName;
        String c1, c2;
    }
}
