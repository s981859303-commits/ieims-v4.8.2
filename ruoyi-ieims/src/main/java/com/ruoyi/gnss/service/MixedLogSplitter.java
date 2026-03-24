package com.ruoyi.gnss.service;

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
 * 混合日志解析器
 *
 * 功能：
 * 1. 解析 NMEA 和 RTCM 混合数据流
 * 2. 调用 RtklibNative 解析 RTCM 观测数据
 * 3. 将数据直接通过原生 SQL 写入 TDengine
 */
@Service
public class MixedLogSplitter {

    private static final Logger logger = LoggerFactory.getLogger(MixedLogSplitter.class);

    // ==================== 配置参数 ====================

    @Value("${gnss.parser.bufferSize:1048576}")
    private int bufferSize;

    @Value("${gnss.parser.maxNmeaLength:1000}")
    private int maxNmeaLength;

    @Value("${gnss.parser.maxRtcmLength:1200}")
    private int maxRtcmLength;

    @Value("${gnss.parser.printIntervalMs:950}")
    private long printIntervalMs;

    // ==================== 依赖注入 (TDengine 存储服务) ====================

    @Autowired(required = false)
    private List<GnssDataListener> listeners = new ArrayList<>();

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

    // ==================== 初始化 ====================

    @javax.annotation.PostConstruct
    public void init() {
        this.buffer = ByteBuffer.allocate(bufferSize);
        logger.info("✅ MixedLogSplitter 初始化完成 ");
        if (rtcmStorageService == null) {
            logger.error("🚨 警告：rtcmStorageService 未注入！请检查 tdengine.enabled 配置！");
        }
    }

    // ==================== 入口方法 ====================

    public void pushData(byte[] newBytes) {
        if (newBytes == null || newBytes.length == 0) return;

        bufferLock.lock();
        try {
            if (buffer.remaining() < newBytes.length) {
                buffer.compact();
                if (buffer.remaining() < newBytes.length) {
                    buffer.clear();
                }
            }

            buffer.put(newBytes);
            buffer.flip();
            parseAndDispatch();
            buffer.compact();

        } catch (Exception e) {
            logger.error("数据接收缓冲区异常: {}", e.getMessage());
            if (buffer != null) buffer.clear();
        } finally {
            bufferLock.unlock();
        }
    }

    // ==================== 拆包逻辑 ====================

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

    // ==================== NMEA 处理 (直接存 TDengine) ====================

    private void notifyNmea(String nmea) {
        if (nmeaStorageService != null) {
            NmeaRecord record = new NmeaRecord(new Date(), nmea);
            nmeaStorageService.saveNmeaData(record);
        }

        if (nmea.startsWith("$GPGGA") || nmea.startsWith("$GNGGA") || nmea.startsWith("$BDGGA")) {
            processGgaData(nmea);
        }

        if (listeners != null) {
            for (GnssDataListener listener : listeners) {
                try { listener.onNmeaReceived(nmea); } catch (Exception ignored) {}
            }
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

            if (gnssStorageService != null) {
                GnssSolution solution = new GnssSolution(new Date(), lat, lon, alt, status, satUsed);
                solution.setHdop(hdop);
                gnssStorageService.saveSolution(solution);
            }
        } catch (Exception e) {
            logger.error("GGA 解析失败: {}", e.getMessage());
        }
    }

    private double nmeaToDecimal(String nmeaPos, String dir) {
        if (nmeaPos == null || nmeaPos.isEmpty()) return 0.0;
        int dotIndex = nmeaPos.indexOf(".");
        if (dotIndex < 2) return 0.0;
        try {
            int deg = Integer.parseInt(nmeaPos.substring(0, dotIndex - 2));
            double min = Double.parseDouble(nmeaPos.substring(dotIndex - 2));
            double decimal = deg + (min / 60.0);
            return ("S".equalsIgnoreCase(dir) || "W".equalsIgnoreCase(dir)) ? -decimal : decimal;
        } catch (Exception e) { return 0.0; }
    }

    // ==================== RTCM 处理 (直接存 TDengine) ====================

    private void notifyRtcm(byte[] data) {
        if (rtcmStorageService != null) {
            rtcmStorageService.saveRtcmRawData(data);
        }

        decodeRtcmData(data);

        if (listeners != null) {
            for (GnssDataListener listener : listeners) {
                try { listener.onRtcmReceived(data); } catch (Exception ignored) {}
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
            logger.error("找不到 rtklib_bridge.dll，请检查系统路径！");
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
            storeCacheToTDengine();
            lastPrintTime = now;
        }
    }

    /**
     * 将 1 秒钟的缓存数据存入 TDengine (移除高频控制台打印)
     */
    private void storeCacheToTDengine() {
        if (satCache.isEmpty()) return;

        if (rtcmStorageService == null) {
            satCache.clear();
            return;
        }

        long currentTimestamp = System.currentTimeMillis();
        int successCount = 0;

        for (SatData d : satCache.values()) {
            try {
                boolean isSaved = rtcmStorageService.saveSatelliteObs(
                        currentTimestamp, d.satName,
                        d.c1, d.c2, d.p1, d.p2, d.l1, d.l2, d.s1
                );
                if (isSaved) successCount++;
            } catch (Exception e) {
                logger.error("❌ 存入数据库失败 [卫星: {}]: {}", d.satName, e.getMessage());
            }
        }

        // 改为 debug 级别，生产环境中默认隐藏这行刷屏日志
        logger.debug("本轮 TDengine 存储完成: 成功写入 {} 颗卫星的数据", successCount);
        satCache.clear();
    }

    // ==================== 内部类 ====================

    private static class SatData {
        int sat;
        Double p1, p2, l1, l2, s1;
        String satName, c1, c2;
    }
}