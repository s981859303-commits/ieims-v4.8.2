package com.ruoyi.gnss.service;

import cn.hutool.json.JSONArray;
import com.ruoyi.common.json.JSONObject;
import com.ruoyi.gnss.domain.GnssSolution;
import com.ruoyi.gnss.domain.NmeaRecord;
import com.ruoyi.gnss.service.impl.TDengineRtcmStorageServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 混合日志解析器（配置修正版）
 *
 * 功能：
 * 1. 解析 NMEA 和 RTCM 混合数据流
 * 2. 调用 RtklibNative 解析 RTCM 观测数据
 * 3. 将数据写入 TDengine（直接写入或通过 MQTT）
 *
 * 配置修正：
 * 1. tdengine-topic 使用配置文件中的 ieims/ieims_data_topic
 * 2. 所有硬编码值改为配置注入
 * 3. 使用 SLF4J 日志框架
 */
@Service
public class MixedLogSplitter {

    private static final Logger logger = LoggerFactory.getLogger(MixedLogSplitter.class);

    // ==================== 配置参数（从配置文件注入）====================

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

    @Value("${gnss.tdengine.enabled:false}")
    private boolean tdengineEnabled;

    // ⭐ TDengine MQTT 主题（从配置文件读取）
    @Value("${gnss.mqtt.tdengineTopic:ieims/ieims_data_topic}")
    private String tdengineMqttTopic;

    // ==================== 依赖注入 ====================

    @Autowired(required = false)
    private List<GnssDataListener> listeners = new ArrayList<>();

    @Autowired(required = false)
    private GnssMqttService mqttService;

    @Autowired(required = false)
    private IGnssStorageService gnssStorageService;

    @Autowired(required = false)
    private INmeaStorageService nmeaStorageService;

    @Autowired(required = false)
    private TDengineRtcmStorageServiceImpl rtcmStorageService;

    // ==================== 缓冲区和锁 ====================

    private ByteBuffer buffer;
    private final ReentrantLock bufferLock = new ReentrantLock();

    // ==================== 卫星数据缓存 ====================

    private final Map<Integer, SatData> satCache = new HashMap<>();
    private long lastPrintTime = System.currentTimeMillis();

    // ==================== 统计计数器 ====================

    private volatile long nmeaCount = 0;
    private volatile long rtcmCount = 0;

    // ==================== 初始化 ====================

    @javax.annotation.PostConstruct
    public void init() {
        this.buffer = ByteBuffer.allocate(bufferSize);
        logger.info("MixedLogSplitter 初始化完成");
        logger.info("  - 缓冲区大小: {} 字节", bufferSize);
        logger.info("  - 站点ID: {}", stationId);
        logger.info("  - TDengine MQTT 主题: {}", tdengineMqttTopic);
        logger.info("  - TDengine 直接写入: {}", tdengineEnabled);
    }

    // ==================== 入口方法 ====================

    /**
     * 入口方法：接收原始字节流
     */
    public void pushData(byte[] newBytes) {
        if (newBytes == null || newBytes.length == 0) return;

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
            logger.error("数据解析异常: {}", e.getMessage());
            if (buffer != null) buffer.clear();
        } finally {
            bufferLock.unlock();
        }
    }

    // ==================== 解析逻辑 ====================

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

    private boolean tryExtractNmea() {
        int startPos = buffer.position() - 1;

        for (int i = buffer.position(); i < buffer.limit(); i++) {
            if (i - startPos > maxNmeaLength) {
                buffer.position(i);
                logger.warn("NMEA 语句过长，已跳过");
                return true;
            }

            if (buffer.get(i) == 0x0A) {
                byte[] lineBytes = new byte[i - startPos + 1];
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
        if (buffer.remaining() < 2) return false;

        int p = buffer.position();
        int length = ((buffer.get(p) & 0x03) << 8) | (buffer.get(p + 1) & 0xFF);

        if (length > maxRtcmLength || length == 0) {
            buffer.reset();
            buffer.get();
            logger.debug("检测到假 RTCM 头，已跳过");
            return true;
        }

        int totalLen = length + 6;

        if (buffer.remaining() < totalLen - 1) return false;

        byte[] rtcmFrame = new byte[totalLen];
        buffer.position(buffer.position() - 1);
        buffer.get(rtcmFrame);

        notifyRtcm(rtcmFrame);
        return true;
    }

    // ==================== NMEA 处理 ====================

    private void notifyNmea(String nmea) {
        nmeaCount++;

        // 分发给监听器
        if (listeners != null) {
            for (GnssDataListener listener : listeners) {
                try {
                    listener.onNmeaReceived(nmea);
                } catch (Exception e) {
                    logger.error("监听器处理 NMEA 异常: {}", e.getMessage());
                }
            }
        }

        // 存储 NMEA 数据
        if (nmeaStorageService != null) {
            NmeaRecord record = new NmeaRecord(new Date(), nmea);
            nmeaStorageService.saveNmeaData(record);
        }

        // 解析 GGA 数据
        if (nmea.startsWith("$GPGGA") || nmea.startsWith("$GNGGA") || nmea.startsWith("$BDGGA")) {
            processGgaData(nmea);
        }
    }

    private void processGgaData(String ggaLine) {
        try {
            String[] parts = ggaLine.split(",", -1);
            if (parts.length < 10 || parts[2].isEmpty() || parts[4].isEmpty()) return;

            double lat = nmeaToDecimal(parts[2], parts[3]);
            double lon = nmeaToDecimal(parts[4], parts[5]);
            int satUsed = parts[7].isEmpty() ? 0 : Integer.parseInt(parts[7]);
            double hdop = parts[8].isEmpty() ? 0.0 : Double.parseDouble(parts[8]);
            double alt = parts[9].isEmpty() ? 0.0 : Double.parseDouble(parts[9]);
            int status = parts[6].isEmpty() ? 0 : Integer.parseInt(parts[6]);

            // 创建解算结果
            GnssSolution solution = new GnssSolution(
                    new Date(), lat, lon, alt, status, satUsed
            );
            solution.setHdop(hdop);

            // 存储到 TDengine
            if (gnssStorageService != null) {
                gnssStorageService.saveSolution(solution);
            }

            // 通过 MQTT 发布（使用配置的主题）
            publishGgaToMqtt(lat, lon, alt, satUsed, hdop);

        } catch (Exception e) {
            logger.error("GGA 解析失败: {}", e.getMessage());
        }
    }

    /**
     * 发布 GGA 数据到 MQTT（使用配置的 tdengineMqttTopic）
     */
    private void publishGgaToMqtt(double lat, double lon, double alt, int satUsed, double hdop) {
        if (mqttService == null || !mqttService.isConnected()) return;

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

        // ⭐ 使用配置文件中的主题
        mqttService.publish(tdengineMqttTopic, finalPayload.toString());
        logger.debug("GGA 数据已发布到主题: {}", tdengineMqttTopic);
    }

    private double nmeaToDecimal(String nmeaPos, String dir) {
        if (nmeaPos == null || nmeaPos.isEmpty()) return 0.0;

        int dotIndex = nmeaPos.indexOf(".");
        if (dotIndex < 2) return 0.0;

        try {
            int deg = Integer.parseInt(nmeaPos.substring(0, dotIndex - 2));
            double min = Double.parseDouble(nmeaPos.substring(dotIndex - 2));
            double decimal = deg + (min / 60.0);

            if ("S".equalsIgnoreCase(dir) || "W".equalsIgnoreCase(dir)) {
                decimal = -decimal;
            }
            return decimal;
        } catch (NumberFormatException e) {
            return 0.0;
        }
    }

    // ==================== RTCM 处理 ====================

    private void notifyRtcm(byte[] data) {
        rtcmCount++;
        logger.debug("识别到 RTCM 帧，长度: {} 字节", data.length);

        // 存储 RTCM 原始数据
        if (rtcmStorageService != null) {
            rtcmStorageService.saveRtcmRawData(data);
        }

        // 调用 RtklibNative 解析
        decodeRtcmData(data);

        // 分发给监听器
        if (listeners != null) {
            for (GnssDataListener listener : listeners) {
                try {
                    listener.onRtcmReceived(data);
                } catch (Exception e) {
                    logger.error("监听器处理 RTCM 异常: {}", e.getMessage());
                }
            }
        }
    }

    private void decodeRtcmData(byte[] rtcmData) {
        try {
            RtklibNative.JavaObs.ByReference obsRef = new RtklibNative.JavaObs.ByReference();
            RtklibNative.JavaObs[] obsArray = (RtklibNative.JavaObs[]) obsRef.toArray(64);

            int count = RtklibNative.INSTANCE.parse_rtcm_frame(rtcmData, rtcmData.length, obsRef, 64);

            if (count > 0) {
                updateSatCache(obsArray, count);
            }
        } catch (UnsatisfiedLinkError e) {
            logger.error("找不到 rtklib_bridge.dll");
        } catch (Exception e) {
            logger.error("RTCM 解码异常: {}", e.getMessage());
        }
    }

    private void updateSatCache(RtklibNative.JavaObs[] newObs, int count) {
        for (int i = 0; i < count; i++) {
            RtklibNative.JavaObs obs = newObs[i];

            if (obs.P[0] == 0.0) continue;

            SatData data = new SatData();
            data.sat = obs.sat;
            data.satName = new String(obs.id).trim();

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

        long now = System.currentTimeMillis();
        if (now - lastPrintTime > printIntervalMs) {
            printAndPublishCache();
            lastPrintTime = now;
        }
    }

    private void printAndPublishCache() {
        if (satCache.isEmpty()) return;

        logger.info("RTCM 数据 | 时间: {} | 卫星数: {}", new Date(), satCache.size());

        long currentTimestamp = System.currentTimeMillis();

        satCache.values().stream()
                .sorted(Comparator.comparingInt(a -> a.sat))
                .forEach(d -> {
                    logger.debug("卫星 {}: P1={}", d.satName, d.p1);

                    // 存储到 TDengine
                    if (rtcmStorageService != null) {
                        rtcmStorageService.saveSatelliteObs(
                                currentTimestamp, d.satName,
                                d.c1, d.c2, d.p1, d.p2, d.l1, d.l2, d.s1
                        );
                    }
                });

        // 通过 MQTT 发布（使用配置的主题）
        publishSatObsToMqtt(currentTimestamp);

        satCache.clear();
    }

    /**
     * 发布卫星观测数据到 MQTT（使用配置的 tdengineMqttTopic）
     */
    private void publishSatObsToMqtt(long timestamp) {
        if (mqttService == null || !mqttService.isConnected()) return;

        JSONArray metricsArray = new JSONArray();

        satCache.values().forEach(d -> {
            JSONObject metric = new JSONObject();
            metric.put("name", "st_satellite_obs");
            metric.put("timestamp", timestamp);

            JSONObject tags = new JSONObject();
            tags.put("station_id", stationId);
            tags.put("sat_name", d.satName);
            metric.put("tags", tags);

            JSONObject fields = new JSONObject();
            if (d.c1 != null) fields.put("c1", d.c1);
            if (d.p1 != null) fields.put("p1", d.p1);
            if (d.l1 != null) fields.put("l1", d.l1);
            if (d.s1 != null) fields.put("s1", d.s1);
            metric.put("fields", fields);

            metricsArray.add(metric);
        });

        JSONObject finalPayload = new JSONObject();
        finalPayload.put("metrics", metricsArray);

        // ⭐ 使用配置文件中的主题
        mqttService.publish(tdengineMqttTopic, finalPayload.toString());
        logger.debug("卫星观测数据已发布到主题: {}", tdengineMqttTopic);
    }

    // ==================== 统计方法 ====================

    public String getStatistics() {
        return String.format("NMEA: %d, RTCM: %d", nmeaCount, rtcmCount);
    }

    // ==================== 内部类 ====================

    private static class SatData {
        int sat;
        Double p1, p2, l1, l2, s1;
        String satName, c1, c2;
    }
}
