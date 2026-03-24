package com.ruoyi.gnss.service;

import cn.hutool.json.JSONArray;
import com.ruoyi.common.json.JSONObject;
import com.ruoyi.gnss.service.GnssDataListener;
import com.ruoyi.gnss.service.MixedLogSplitter;
import com.ruoyi.gnss.service.RtklibNative;
import com.ruoyi.gnss.service.impl.TDengineNmeaStorageServiceImpl;
import com.ruoyi.gnss.service.impl.TDengineRtcmStorageServiceImpl;
import com.ruoyi.gnss.service.impl.TDengineRtcmStorageServiceImpl.SatObsData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/**
 * GNSS 混合数据流解析器
 *
 * <p>
 * 负责解析混合数据流中的 NMEA 和 RTCM 数据，并将数据存储到 TDengine 数据库。
 * 支持的数据类型：
 * - NMEA: $GPGGA, $GNGGA, $BDGGA 等定位语句
 * - RTCM: 1074 (GPS MSM4), 1127 (BeiDou MSM4) 等观测值消息
 * </p>
 *
 * <p>
 * 数据存储：
 * - NMEA 数据存储到 st_receiver_status 表（接收机基础状态）
 * - RTCM 观测数据存储到 st_sat_obs 表（卫星原始观测数据）
 * </p>
 *
 * @author GNSS Team
 * @date 2026-03-23
 */
@Service
public class MixedLogSplitter {

    private static final Logger log = LoggerFactory.getLogger(MixedLogSplitter.class);

    /** 默认站点ID */
    private static final String DEFAULT_STATION_ID = "8900_1";

    private Map<Integer, RtklibNative.JavaObs> epochCache = new HashMap<>();
    private long lastEpochTime = 0;

    // 注入监听器列表（支持多个接收者，如推送到RTKLIB的服务、存数据库的服务）
    @Autowired(required = false)
    private List<GnssDataListener> listeners = new ArrayList<>();

    // 1MB 缓冲区
    private ByteBuffer buffer = ByteBuffer.allocate(1024 * 1024);

    // 注入你的 MQTT 服务
    @Autowired
    private GnssMqttService mqttService;

    // 🔥 新增：注入 TDengine 存储服务
    @Autowired
    private TDengineNmeaStorageServiceImpl nmeaStorageService;

    @Autowired
    private TDengineRtcmStorageServiceImpl rtcmStorageService;

    // 定义用于发送给 TDengine 的专用 MQTT 主题
    private static final String TDENGINE_MQTT_TOPIC = "tdengine/rtcm/data";

    /**
     * 入口方法：接收网络/串口传来的原始字节流
     */
    public synchronized void pushData(byte[] newBytes) {
        log.debug("📥 [Debug] MixedLogSplitter 收到数据: {} 字节", newBytes.length);
        try {
            if (buffer.remaining() < newBytes.length) {
                buffer.compact();
                if (buffer.remaining() < newBytes.length) buffer.clear();
            }
            buffer.put(newBytes);
            buffer.flip();
            parseAndDispatch(); // 解析分包
            buffer.compact();
        } catch (Exception e) {
            log.error("数据解析异常: {}", e.getMessage(), e);
        }
    }

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
                    break; // 数据不足，跳出循环等待下一次 pushData
                }
            }
        }
    }

    private boolean tryExtractNmea() {
        int startPos = buffer.position() - 1;
        for (int i = buffer.position(); i < buffer.limit(); i++) {
            if (i - startPos > 1000) { buffer.position(i); return true; }
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

    private boolean tryExtractRtcm() {
        if (buffer.remaining() < 2) { buffer.reset(); return false; }

        int p = buffer.position();
        // 解析 RTCM 长度 (前3个字节包含长度信息，RTCM3 长度在 Byte1 的后6位和 Byte2)
        int length = ((buffer.get(p) & 0x03) << 8) | (buffer.get(p + 1) & 0xFF);

        if (length > 1200 || length == 0) {
            buffer.reset(); buffer.get(); return true; // 假头，跳过
        }

        // RTCM 总长度 = Header(3) + Body(length) + CRC(3)
        int totalLen = length + 6;

        if (buffer.remaining() < totalLen - 1) {
            buffer.reset(); return false; // 数据不够，等待更多数据
        }

        byte[] rtcmFrame = new byte[totalLen];
        buffer.position(buffer.position() - 1); // 回退到 0xD3 的位置
        buffer.get(rtcmFrame);

        // 这里提取出了完整的 RTCM 帧，直接分发
        notifyRtcm(rtcmFrame);

        return true;
    }

    // --- 内部通知方法 ---

    private void notifyNmea(String nmea) {
        // 分发给原有监听器
        if (listeners != null) {
            for (GnssDataListener listener : listeners) {
                try { listener.onNmeaReceived(nmea); } catch (Exception e) {
                    log.error("监听器处理NMEA异常: {}", e.getMessage());
                }
            }
        }

        // 🔥 新增：如果是 GGA 数据，解析并存储到 TDengine
        if (nmea.startsWith("$GPGGA") || nmea.startsWith("$GNGGA") || nmea.startsWith("$BDGGA")) {
            parseAndStoreGgaToTDengine(nmea);
        }
    }

    /**
     * 解析 NMEA GGA 数据并存储到 TDengine
     *
     * @param ggaLine NMEA GGA 语句
     */
    private void parseAndStoreGgaToTDengine(String ggaLine) {
        try {
            String[] parts = ggaLine.split(",", -1);
            // 校验 GGA 格式基本完整且经纬度不为空
            if (parts.length < 10 || parts[2].isEmpty() || parts[4].isEmpty()) return;

            // NMEA 的经纬度是度分格式，需要转成十进制度
            double lat = nmeaToDecimal(parts[2], parts[3]);
            double lon = nmeaToDecimal(parts[4], parts[5]);

            int satUsed = parts[7].isEmpty() ? 0 : Integer.parseInt(parts[7]);
            double hdop = parts[8].isEmpty() ? 0.0 : Double.parseDouble(parts[8]);
            double alt = parts[9].isEmpty() ? 0.0 : Double.parseDouble(parts[9]);

            long timestamp = System.currentTimeMillis();

            // 🔥 核心：调用 TDengine 存储服务保存数据
            if (nmeaStorageService != null) {
                nmeaStorageService.saveReceiverStatus(
                        DEFAULT_STATION_ID,
                        timestamp,
                        lat,
                        lon,
                        alt,
                        satUsed,
                        hdop
                );
                log.info("✅ [NMEA] 成功存储接收机坐标到 TDengine: Lat={}, Lon={}, Alt={}m, 卫星数={}",
                        lat, lon, alt, satUsed);
            }

            // 同时保留 MQTT 推送（可选）
            if (mqttService != null) {
                JSONObject metric = new JSONObject();
                metric.put("name", "st_receiver_status");
                metric.put("timestamp", timestamp);

                JSONObject tags = new JSONObject();
                tags.put("station_id", DEFAULT_STATION_ID);
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

                mqttService.publish(TDENGINE_MQTT_TOPIC, finalPayload.toString());
            }

        } catch (Exception e) {
            log.error("⚠️ GGA 解析失败: {}", e.getMessage());
        }
    }

    // 辅助方法：经纬度度分转十进制
    private double nmeaToDecimal(String nmeaPos, String dir) {
        if (nmeaPos == null || nmeaPos.isEmpty()) return 0.0;
        int dotIndex = nmeaPos.indexOf(".");
        if (dotIndex < 2) return 0.0;

        // 提取度和分
        int deg = Integer.parseInt(nmeaPos.substring(0, dotIndex - 2));
        double min = Double.parseDouble(nmeaPos.substring(dotIndex - 2));
        double decimal = deg + (min / 60.0);

        // 南半球和西半球取负
        if ("S".equalsIgnoreCase(dir) || "W".equalsIgnoreCase(dir)) {
            decimal = -decimal;
        }
        return decimal;
    }

    private void notifyRtcm(byte[] data) {
        log.debug("📦 [Debug] 识别到完整 RTCM 帧，长度: {}，准备交给 DLL 解算...", data.length);

        // 1. 调用 JNA 解算
        decodeAndPrintRtcm(data);

        // 2. 分发给其他业务监听器
        if (listeners == null || listeners.isEmpty()) {
            return;
        }

        for (GnssDataListener listener : listeners) {
            try {
                listener.onRtcmReceived(data);
            } catch (Exception e) {
                log.error("❌ 推送异常: {}", e.getMessage());
            }
        }
    }

    /**
     * JNA 调用逻辑：解算 RTCM 并输出到控制台
     */
    private void decodeAndPrintRtcm(byte[] rtcmData) {
        try {
            // 1. 准备内存空间：一次最多接收 64 颗卫星的数据
            RtklibNative.JavaObs.ByReference obsRef = new RtklibNative.JavaObs.ByReference();
            RtklibNative.JavaObs[] obsArray = (RtklibNative.JavaObs[]) obsRef.toArray(64);

            // 2. 调用 C 语言 DLL 函数
            int count = RtklibNative.INSTANCE.parse_rtcm_frame(rtcmData, rtcmData.length, obsRef, 64);

            // 3. 如果 count > 0，说明是观测值帧 (1074/1127 等)
            if (count > 0) {
                updateEpochCache(obsArray, count);
            }
        } catch (UnsatisfiedLinkError e) {
            log.error("❌ 严重错误：找不到 rtklib_bridge.dll 文件！请确保它在项目根目录下。");
        } catch (Exception e) {
            log.error("RTCM 解码异常: {}", e.getMessage(), e);
        }
    }

    /**
     * 辅助工具：将 RTKLIB 的 int 卫星号转为可读字符串
     */
    private String getSatName(int satId) {
        // GPS: 1 ~ 32
        if (satId >= 1 && satId <= 32) {
            return String.format("G%02d", satId);
        }
        // SBAS: 33 ~ 55
        if (satId >= 33 && satId <= 55) {
            return String.format("S%02d", satId);
        }
        // GLONASS: 56 ~ 85
        if (satId >= 56 && satId <= 85) {
            return String.format("R%02d", satId - 55);
        }
        // Galileo: 86 ~ 122
        if (satId >= 86 && satId <= 122) {
            return String.format("E%02d", satId - 85);
        }
        // BeiDou: 140 ~ 190
        if (satId >= 140 && satId <= 190) {
            return String.format("C%02d", satId - 140);
        }
        return "SAT" + satId;
    }

    // 定义一个简单的内部类来存数据
    class SatData {
        int sat;
        Double p1, p2, l1, l2, s1;
        String satName;
        String c1, c2;
    }

    // 缓存池：Key是卫星号，Value是数据
    private Map<Integer, SatData> satCache = new HashMap<>();
    private long lastPrintTime = System.currentTimeMillis();

    /**
     * 核心逻辑：聚合 + 定时打印
     */
    private void updateEpochCache(RtklibNative.JavaObs[] newObs, int count) {
        // 1. 把新收到的卫星存入缓存 Map (去重)
        for (int i = 0; i < count; i++) {
            RtklibNative.JavaObs obs = newObs[i];

            // 过滤无效数据
            if (obs.P[0] == 0.0) continue;

            SatData data = new SatData();
            data.sat = obs.sat;
            data.satName = new String(obs.id).trim();

            data.p1 = obs.P[0];
            data.l1 = obs.L[0];

            // P2/L2: 严格读取 index 1
            data.p2 = (obs.P[1] != 0.0) ? obs.P[1] : null;
            data.l2 = (obs.L[1] != 0.0) ? obs.L[1] : null;

            // SNR 修正
            data.s1 = (obs.snr[0] > 0) ? obs.snr[0] * 4.0 : 0;

            satCache.put(data.sat, data);
        }

        // 2. 检查时间，如果距离上次打印超过 950ms，就打印一次完整列表
        long now = System.currentTimeMillis();
        if (now - lastPrintTime > 950) {
            printCacheAndStoreToTDengine();
            lastPrintTime = now;
        }
    }

    /**
     * 打印缓存数据并存储到 TDengine
     */
    private void printCacheAndStoreToTDengine() {
        if (satCache.isEmpty()) return;

        log.info("=======================================================================================================");
        log.info("🚀 RTCM 实时数据 | 时间: {} | 总星数: {}", new java.util.Date(), satCache.size());
        log.info(String.format("%-6s %-4s %-4s %-8s %-14s %-14s %-14s %-14s",
                "SAT", "C1", "C2", "S1(dBHz)", "P1(m)", "P2(m)", "L1(cycle)", "L2(cycle)"));
        log.info("-------------------------------------------------------------------------------------------------------");

        // 🔥 准备存储到 TDengine 的数据列表
        List<SatObsData> obsDataList = new ArrayList<>();
        long currentTimestamp = System.currentTimeMillis();

        satCache.values().stream()
                .sorted((a, b) -> Integer.compare(a.sat, b.sat))
                .forEach(d -> {
                    // 控制台打印
                    log.info(String.format("%-6s %-4s %-4s %-8s %-14s %-14s %-14s %-14s",
                            d.satName, d.c1, d.c2,
                            formatVal(d.s1), formatVal(d.p1), formatVal(d.p2),
                            formatVal(d.l1), formatVal(d.l2)));

                    // 🔥 构建 TDengine 存储数据
                    SatObsData obsData = new SatObsData();
                    obsData.satName = d.satName;
                    obsData.elevation = 0.0;  // 仰角需要从星历数据获取
                    obsData.azimuth = 0.0;    // 方位角需要从星历数据获取
                    obsData.c1 = d.c1 != null ? d.c1 : "";
                    obsData.snr1 = d.s1 != null ? d.s1 : 0.0;
                    obsData.p1 = d.p1 != null ? d.p1 : 0.0;
                    obsData.l1 = d.l1 != null ? d.l1 : 0.0;
                    obsData.c2 = d.c2 != null ? d.c2 : "";
                    obsData.snr2 = 0.0;
                    obsData.p2 = d.p2 != null ? d.p2 : 0.0;
                    obsData.l2 = d.l2 != null ? d.l2 : 0.0;

                    obsDataList.add(obsData);
                });

        log.info("=======================================================================================================");

        // 🔥 核心：批量存储到 TDengine
        if (rtcmStorageService != null && !obsDataList.isEmpty()) {
            rtcmStorageService.saveSatObsDataBatch(DEFAULT_STATION_ID, currentTimestamp, obsDataList);
            log.info("✅ [RTCM] 成功存储 {} 颗卫星观测数据到 TDengine", obsDataList.size());
        }

        // 同时保留 MQTT 推送（可选）
        try {
            if (mqttService != null) {
                JSONArray metricsArray = new JSONArray();

                for (SatObsData d : obsDataList) {
                    JSONObject metric = new JSONObject();
                    metric.put("name", "st_sat_obs");
                    metric.put("timestamp", currentTimestamp);

                    JSONObject tags = new JSONObject();
                    tags.put("station_id", DEFAULT_STATION_ID);
                    tags.put("sat_name", d.satName);
                    metric.put("tags", tags);

                    JSONObject fields = new JSONObject();
                    if (d.c1 != null && !d.c1.isEmpty()) fields.put("c1", d.c1);
                    if (d.c2 != null && !d.c2.isEmpty()) fields.put("c2", d.c2);
                    if (d.snr1 > 0) fields.put("snr1", d.snr1);
                    if (d.p1 > 0) fields.put("p1", d.p1);
                    if (d.p2 > 0) fields.put("p2", d.p2);
                    if (d.l1 != 0) fields.put("l1", d.l1);
                    if (d.l2 != 0) fields.put("l2", d.l2);

                    metric.put("fields", fields);
                    metricsArray.add(metric);
                }

                JSONObject finalPayload = new JSONObject();
                finalPayload.put("metrics", metricsArray);
                mqttService.publish(TDENGINE_MQTT_TOPIC, finalPayload.toString());
            }
        } catch (Exception e) {
            log.error("❌ 组装或发送 MQTT 数据失败: {}", e.getMessage());
        }

        // 清空缓存
        satCache.clear();
    }

    // 辅助方法：处理 Double 转 String
    private String formatVal(Double val) {
        if (val == null || val == 0.0) {
            return "null";
        }
        return String.format(java.util.Locale.US, "%.3f", val);
    }
}
