package com.ruoyi.gnss.service;

import com.ruoyi.gnss.domain.GnssSolution;
import com.ruoyi.gnss.domain.NmeaRecord;
import com.ruoyi.gnss.service.impl.SatelliteDataFusionService;
import com.sun.jna.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 混合日志解析器 (多站点安全版)
 *
 * 功能：
 * 1. 解析 NMEA 和 RTCM 混合数据流
 * 2. 调用 RtklibNative 解析 RTCM 观测数据
 * 3. 解析 GSV 语句获取卫星仰角、方位角、信噪比
 * 4. 融合 GSV 和 RTCM 数据
 * 5. 将融合后的数据写入 TDengine
 *
 * 核心修复：
 * 1. 使用 StationState 隔离每个站点的 ByteBuffer 和历元时间，防止多线程污染。
 * 2. 修复 JNA 结构体数组 read() 同步问题，确保提取所有卫星数据。
 *
 * @author GNSS Team
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

    @Value("${gnss.parser.stationId:8900_1}")
    private String defaultStationId;

    // ==================== 依赖注入 ====================

    @Autowired(required = false)
    private List<GnssDataListener> listeners = new ArrayList<>();

    @Autowired(required = false)
    private IGnssStorageService gnssStorageService;

    @Autowired(required = false)
    private INmeaStorageService nmeaStorageService;

    @Autowired(required = false)
    private IRtcmStorageService rtcmStorageService;

    @Autowired(required = false)
    private GsvParser gsvParser;

    @Autowired(required = false)
    private SatelliteDataFusionService fusionService;

    @Autowired(required = false)
    private GnssAsyncProcessor asyncProcessor;

    @Autowired
    private RtklibContextManager contextManager;

    // ==================== 多站点状态隔离 ====================

    /**
     * 内部类：管理单个站点的独立解析状态
     */
    private static class StationState {
        final ByteBuffer buffer;
        final ReentrantLock bufferLock = new ReentrantLock();
        volatile Long currentEpochTime = null;
        final AtomicLong lastPrintTime = new AtomicLong(System.currentTimeMillis());
        final AtomicLong overflowCount = new AtomicLong(0);

        StationState(int size) {
            this.buffer = ByteBuffer.allocate(size);
        }
    }

    // 存储所有站点的状态映射
    private final ConcurrentHashMap<String, StationState> stationStates = new ConcurrentHashMap<>();

    // ==================== 统计信息 ====================

    private final AtomicLong globalBufferOverflowCount = new AtomicLong(0);
    private final AtomicLong nmeaCount = new AtomicLong(0);
    private final AtomicLong rtcmCount = new AtomicLong(0);

    // ==================== 缓存的字符串常量 ====================

    private static final String PREFIX_GNGGA = "$GNGGA";
    private static final String PREFIX_GPGGA = "$GPGGA";
    private static final String PREFIX_BDGGA = "$BDGGA";

    // ==================== 初始化 ====================

    @PostConstruct
    public void init() {
        StationContext.setDefaultStationId(defaultStationId);

        try {
            Pointer ctx = contextManager.getOrCreateContext(defaultStationId);
            logger.info("默认站点 Context 已创建: stationId={}, ptr={}", defaultStationId, ctx);
        } catch (Exception e) {
            logger.warn("创建默认站点 Context 失败: {}", e.getMessage());
        }

        logger.info("MixedLogSplitter 初始化完成（多站点安全版），缓冲区大小: {} bytes, 默认站点: {}, DLL版本: {}",
                bufferSize, defaultStationId, contextManager.getDllVersion());

        if (rtcmStorageService == null) logger.warn("警告：rtcmStorageService 未注入");
        if (gsvParser == null) logger.warn("警告：gsvParser 未注入，GSV 解析功能将不可用");
        if (fusionService == null) logger.warn("警告：fusionService 未注入，数据融合功能将不可用");
        if (asyncProcessor == null) logger.warn("警告：asyncProcessor 未注入，将使用同步模式");
    }

    // ==================== 入口方法 ====================

    public void pushData(byte[] newBytes) {
        pushData(defaultStationId, newBytes);
    }

    public void pushData(String stationId, byte[] newBytes) {
        if (newBytes == null || newBytes.length == 0) return;

        // 获取该站点独占的状态对象
        StationState state = stationStates.computeIfAbsent(stationId, k -> new StationState(bufferSize));

        state.bufferLock.lock();
        try {
            if (!tryEnsureCapacity(state, newBytes.length)) {
                long overflowCount = state.overflowCount.incrementAndGet();
                globalBufferOverflowCount.incrementAndGet();
                if (overflowCount % 100 == 1) {
                    logger.warn("缓冲区溢出，丢弃数据包，长度: {} bytes，该站点累计溢出: {} 次，站点: {}",
                            newBytes.length, overflowCount, stationId);
                }
                return;
            }

            state.buffer.put(newBytes);
            state.buffer.flip();

            // 在站点上下文中解析
            try (StationContext.Scope scope = StationContext.withStation(stationId)) {
                parseAndDispatch(stationId, state);
            }

            state.buffer.compact();

        } catch (Exception e) {
            logger.error("数据接收缓冲区异常: {}", e.getMessage());
            if (state.buffer != null) {
                state.buffer.clear();
            }
        } finally {
            state.bufferLock.unlock();
        }
    }

    private boolean tryEnsureCapacity(StationState state, int required) {
        ByteBuffer buffer = state.buffer;
        if (buffer.remaining() >= required) {
            return true;
        }

        buffer.compact();
        buffer.flip();
        buffer.compact();

        if (buffer.remaining() >= required) {
            return true;
        }

        if (required > buffer.capacity()) {
            logger.error("数据包过大，超过缓冲区容量: {} > {}", required, buffer.capacity());
            return false;
        }

        int lostBytes = buffer.position();
        if (lostBytes > 0) {
            logger.warn("缓冲区已满，丢弃 {} bytes 未处理的半包数据", lostBytes);
        }
        buffer.clear();

        return buffer.remaining() >= required;
    }

    // ==================== 拆包逻辑 ====================

    private void parseAndDispatch(String stationId, StationState state) {
        ByteBuffer buffer = state.buffer;
        while (buffer.hasRemaining()) {
            buffer.mark();
            byte b = buffer.get();

            if (b == 0x24) { // NMEA ($)
                if (!tryExtractNmea(stationId, state)) {
                    buffer.reset();
                    break;
                }
            } else if ((b & 0xFF) == 0xD3) { // RTCM (0xD3)
                if (!tryExtractRtcm(stationId, state)) {
                    buffer.reset();
                    break;
                }
            }
        }
    }

    private boolean tryExtractNmea(String stationId, StationState state) {
        ByteBuffer buffer = state.buffer;
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
                notifyNmea(stationId, nmea, state);

                buffer.position(i + 1);
                return true;
            }
        }
        return false;
    }

    private boolean tryExtractRtcm(String stationId, StationState state) {
        ByteBuffer buffer = state.buffer;
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

        notifyRtcm(stationId, rtcmFrame, state);
        return true;
    }

    // ==================== NMEA 处理 ====================

    private void notifyNmea(String stationId, String nmea, StationState state) {
        nmeaCount.incrementAndGet();

        if (asyncProcessor != null) {
            asyncProcessor.submitNmea(stationId, nmea);
        } else if (nmeaStorageService != null) {
            nmeaStorageService.saveNmeaData(stationId, new NmeaRecord(new Date(), nmea));
        }

        if (isGgaSentence(nmea)) {
            processGgaData(stationId, nmea, state);
        }

        if (fusionService != null && gsvParser != null && gsvParser.isGsvSentence(nmea)) {
            fusionService.processGsvData(stationId, nmea);
        }

        if (listeners != null) {
            for (GnssDataListener listener : listeners) {
                try {
                    listener.onNmeaReceived(nmea);
                } catch (Exception ignored) {}
            }
        }
    }

    private boolean isGgaSentence(String nmea) {
        return nmea.startsWith(PREFIX_GNGGA) ||
                nmea.startsWith(PREFIX_GPGGA) ||
                nmea.startsWith(PREFIX_BDGGA);
    }

    private void processGgaData(String stationId, String ggaLine, StationState state) {
        try {
            String[] parts = ggaLine.split(",", -1);

            if (parts.length < 10) return;
            if (parts[2] == null || parts[2].isEmpty() || parts[4] == null || parts[4].isEmpty()) return;

            // 提取 UTC 时间作为历元时间，存入当前站点的专属状态
            if (parts.length > 1 && parts[1] != null && !parts[1].isEmpty()) {
                state.currentEpochTime = parseUtcTime(parts[1]);
                if (fusionService != null) {
                    fusionService.setEpochTime(stationId, state.currentEpochTime);
                }
            }

            double lat = nmeaToDecimal(parts[2], parts[3]);
            double lon = nmeaToDecimal(parts[4], parts[5]);
            int satUsed = safeParseInt(parts[7], 0);
            double hdop = safeParseDouble(parts[8], 0.0);
            double alt = safeParseDouble(parts[9], 0.0);
            int status = safeParseInt(parts[6], 0);

            if (gnssStorageService != null) {
                GnssSolution solution = new GnssSolution(new Date(), lat, lon, alt, status, satUsed);
                solution.setHdop(hdop);

                if (asyncProcessor != null) {
                    asyncProcessor.submitGnssSolution(solution);
                } else {
                    gnssStorageService.saveSolution(solution);
                }
            }
        } catch (Exception e) {
            logger.error("GGA 解析失败: {}", e.getMessage());
        }
    }

    private Long parseUtcTime(String utcTime) {
        if (utcTime == null || utcTime.length() < 6) return null;
        try {
            Calendar cal = Calendar.getInstance();
            int hours = (utcTime.charAt(0) - '0') * 10 + (utcTime.charAt(1) - '0');
            int minutes = (utcTime.charAt(2) - '0') * 10 + (utcTime.charAt(3) - '0');
            int seconds = (utcTime.charAt(4) - '0') * 10 + (utcTime.charAt(5) - '0');
            int millis = 0;
            if (utcTime.length() > 7 && utcTime.charAt(6) == '.') {
                String decimal = utcTime.substring(7);
                millis = (int) (Double.parseDouble("0." + decimal) * 1000);
            }
            cal.set(Calendar.HOUR_OF_DAY, hours);
            cal.set(Calendar.MINUTE, minutes);
            cal.set(Calendar.SECOND, seconds);
            cal.set(Calendar.MILLISECOND, millis);
            return cal.getTimeInMillis();
        } catch (Exception e) {
            return null;
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
        } catch (Exception e) {
            return 0.0;
        }
    }

    private int safeParseInt(String value, int defaultValue) {
        if (value == null || value.isEmpty()) return defaultValue;
        try { return Integer.parseInt(value); } catch (NumberFormatException e) { return defaultValue; }
    }

    private double safeParseDouble(String value, double defaultValue) {
        if (value == null || value.isEmpty()) return defaultValue;
        try { return Double.parseDouble(value); } catch (NumberFormatException e) { return defaultValue; }
    }

    // ==================== RTCM 处理 ====================

    private void notifyRtcm(String stationId, byte[] data, StationState state) {
        rtcmCount.incrementAndGet();

        if (asyncProcessor != null) {
            asyncProcessor.submitRtcm(stationId, data);
        } else if (rtcmStorageService != null) {
            rtcmStorageService.saveRtcmRawData(data);
        }

        decodeRtcmData(stationId, data, state);

        if (listeners != null) {
            for (GnssDataListener listener : listeners) {
                try {
                    listener.onRtcmReceived(data);
                } catch (Exception ignored) {}
            }
        }
    }

    private void decodeRtcmData(String stationId, byte[] rtcmData, StationState state) {
        try {
            Pointer ctx = contextManager.getOrCreateContext(stationId);
            if (ctx == null) {
                logger.error("无法获取站点 Context: stationId={}", stationId);
                return;
            }

            RtklibNative.JavaObs.ByReference obsRef = new RtklibNative.JavaObs.ByReference();
            int maxObs = 64;
            RtklibNative.JavaObs[] obsArray = (RtklibNative.JavaObs[]) obsRef.toArray(maxObs);

            int count = RtklibNative.INSTANCE.rtklib_parse_rtcm_frame_ex(ctx, rtcmData, rtcmData.length, obsRef, maxObs);

            if (count > 0) {
                // 防御性拦截
                if (count > maxObs) count = maxObs;

                // 【核心修复】JNA 数据提取：手动将底层 C 内存同步到 Java 数组
                for (int i = 0; i < count; i++) {
                    obsArray[i].read();
                }

                updateSatCache(stationId, obsArray, count, rtcmData, state);
            }
        } catch (UnsatisfiedLinkError e) {
            logger.error("找不到 rtklib_bridge.dll，请检查系统路径！");
        } catch (Exception e) {
            logger.error("RTCM 解码异常: stationId={}, error={}", stationId, e.getMessage());
        }
    }

    private void updateSatCache(String stationId, RtklibNative.JavaObs[] newObs, int count, byte[] rtcmData, StationState state) {
        int rtcmMessageType = getRtcmMessageType(rtcmData);

        for (int i = 0; i < count; i++) {
            RtklibNative.JavaObs obs = newObs[i];
            if (obs.P[0] == 0.0) continue;

            String satName = new String(obs.id).trim();
            String satNo = SatNoConverter.fromRtcmSatId(satName, rtcmMessageType);

            Double p1 = obs.P[0] > 0 ? obs.P[0] : null;
            Double l1 = obs.L[0] != 0.0 ? obs.L[0] : null;
            Double p2 = obs.P[1] > 0 ? obs.P[1] : null;
            Double l2 = obs.L[1] != 0.0 ? obs.L[1] : null;
            Double snr = obs.snr[0] > 0 ? (double)(obs.snr[0] * 4.0) : null;

            String c1 = null, c2 = null;
            if (obs.code != null && obs.code.length >= 8) {
                c1 = new String(obs.code, 0, 8).trim();
            }
            if (obs.code != null && obs.code.length >= 16) {
                c2 = new String(obs.code, 8, 8).trim();
            }

            if (fusionService != null) {
                fusionService.processRtcmData(stationId, satNo, rtcmMessageType, p1, p2, l1, l2, snr, c1, c2);
            }
        }

        // 定时触发融合入库 (按站点独立触发)
        long now = System.currentTimeMillis();
        long lastTime = state.lastPrintTime.get();
        if (now - lastTime > printIntervalMs) {
            if (state.lastPrintTime.compareAndSet(lastTime, now)) {
                if (fusionService != null) {
                    fusionService.fuseAndStore(stationId);
                }
            }
        }
    }

    private int getRtcmMessageType(byte[] rtcmData) {
        if (rtcmData == null || rtcmData.length < 6) return 0;
        return ((rtcmData[3] & 0xFF) << 4) | ((rtcmData[4] & 0xF0) >> 4);
    }

    // ==================== 公共接口 ====================

    public Long getCurrentEpochTime(String stationId) {
        StationState state = stationStates.get(stationId);
        return state != null ? state.currentEpochTime : null;
    }

    public long getBufferOverflowCount() {
        return globalBufferOverflowCount.get();
    }

    public long getNmeaCount() { return nmeaCount.get(); }
    public long getRtcmCount() { return rtcmCount.get(); }
    public int getActiveContextCount() { return contextManager.getActiveContextCount(); }
    public java.util.Set<String> getAllStationIds() { return contextManager.getAllStationIds(); }
    public boolean resetStationContext(String stationId) { return contextManager.resetContext(stationId); }

    public String getStatistics() {
        if (asyncProcessor != null) {
            return asyncProcessor.getStatistics() +
                    String.format(", Contexts=%d", getActiveContextCount());
        }
        return String.format("NMEA=%d, RTCM=%d, Overflow=%d, Contexts=%d",
                nmeaCount.get(), rtcmCount.get(), globalBufferOverflowCount.get(), getActiveContextCount());
    }
}