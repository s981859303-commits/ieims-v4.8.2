package com.ruoyi.ieims.gnss.service;

import com.ruoyi.ieims.gnss.domain.GnssSolution;
import com.ruoyi.ieims.gnss.domain.GsvSatelliteData;
import com.ruoyi.ieims.gnss.service.impl.SatelliteDataFusionService;
import com.sun.jna.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 混合日志解析器
 *
 * 功能：
 * 1. 解析 NMEA 和 RTCM 混合数据流
 * 2. 调用 RtklibNative 解析 RTCM 观测数据
 * 3. 提取真实的 GNSS 历元时间进行数据对齐
 * 4. 融合 GSV 和 RTCM 数据，关联完整日期时间
 *
 * @version 2.5 - 2026-04-28 修复 Context 生命周期导致无法 Fixed 的致命 Bug；引入 JNA 享元模式彻底消除 GC 瓶颈
 *  */
@Service
public class MixedLogSplitter {

    @Autowired
    private GgaParser ggaParser;

    private static final Logger logger = LoggerFactory.getLogger(MixedLogSplitter.class);

    // ==================== 常量定义 ====================

    private static final int DEFAULT_BUFFER_SIZE = 4096;

    @Value("${gnss.parser.maxBufferSize:65536}")
    private int maxBufferSize;

    @Value("${gnss.rtklib.maxObs:64}")
    private int maxObs;

    private static final byte NMEA_START = '$';
    private static final byte RTCM3_PREAMBLE = (byte) 0xD3;

    // RTCM3 CRC24Q 查找表，用于快速校验伪造帧
    private static final int[] CRC24Q_TABLE = new int[256];
    static {
        for (int i = 0; i < 256; i++) {
            int crc = i << 16;
            for (int j = 0; j < 8; j++) {
                crc <<= 1;
                if ((crc & 0x1000000) != 0) crc ^= 0x1864CFB;
            }
            CRC24Q_TABLE[i] = crc & 0xFFFFFF;
        }
    }

    // ==================== NMEA 入库白名单 ====================

    private static final Set<String> NMEA_STORAGE_WHITELIST = new HashSet<>();
    static {
        NMEA_STORAGE_WHITELIST.add("GGA");
        NMEA_STORAGE_WHITELIST.add("GSV");
        NMEA_STORAGE_WHITELIST.add("ZDA");
    }

    private static final Set<String> NMEA_STORAGE_WHITELIST_FULL = new HashSet<>();
    static {
        NMEA_STORAGE_WHITELIST_FULL.add("$GNGGA");
        NMEA_STORAGE_WHITELIST_FULL.add("$GPGGA");
        NMEA_STORAGE_WHITELIST_FULL.add("$BDGGA");
        NMEA_STORAGE_WHITELIST_FULL.add("$GLGGA");
        NMEA_STORAGE_WHITELIST_FULL.add("$GAGGA");

        NMEA_STORAGE_WHITELIST_FULL.add("$GPGSV");
        NMEA_STORAGE_WHITELIST_FULL.add("$GBGSV");
        NMEA_STORAGE_WHITELIST_FULL.add("$BDGSV");
        NMEA_STORAGE_WHITELIST_FULL.add("$GLGSV");
        NMEA_STORAGE_WHITELIST_FULL.add("$GAGSV");
        NMEA_STORAGE_WHITELIST_FULL.add("$QZGSV");
        NMEA_STORAGE_WHITELIST_FULL.add("$IGSV");
        NMEA_STORAGE_WHITELIST_FULL.add("$GNGSV");

        NMEA_STORAGE_WHITELIST_FULL.add("$GNZDA");
        NMEA_STORAGE_WHITELIST_FULL.add("$GPZDA");
        NMEA_STORAGE_WHITELIST_FULL.add("$BDZDA");
    }

    // ==================== 依赖注入 ====================

    @Autowired(required = false)
    private GnssAsyncProcessor asyncProcessor;

    @Autowired(required = false)
    private SatelliteDataFusionService fusionService;

    @Autowired
    private GsvParser gsvParser;

    @Autowired
    private ZdaParser zdaParser;

    @Autowired
    private RtklibContextManager contextManager;

    @Value("${gnss.parser.stationId:8900_1}")
    private String defaultStationId;

    @Value("${gnss.parser.bufferSize:4096}")
    private int bufferSize;

    // ==================== 状态管理 ====================

    private final ConcurrentHashMap<String, StationState> stationStates = new ConcurrentHashMap<>();

    private final AtomicLong nmeaCount = new AtomicLong(0);
    private final AtomicLong rtcmCount = new AtomicLong(0);
    private final AtomicLong gsvCount = new AtomicLong(0);
    private final AtomicLong zdaCount = new AtomicLong(0);
    private final AtomicLong globalBufferOverflowCount = new AtomicLong(0);

    // 入库统计
    private final AtomicLong nmeaStorageCount = new AtomicLong(0);
    private final AtomicLong nmeaFilteredCount = new AtomicLong(0);

    @PostConstruct
    public void init() {
        logger.info("MixedLogSplitter 初始化完成，默认站点: {}", defaultStationId);
        getOrCreateStationState(defaultStationId);
    }

    // ==================== 公共接口 ====================

    public void pushData(byte[] data) {
        pushDataWithStation(defaultStationId, data);
    }

    public void pushDataWithStation(String stationId, byte[] data) {
        if (data == null || data.length == 0) {
            return;
        }

        StationState state = getOrCreateStationState(stationId);
        state.lock.lock();
        try {
            processBuffer(state, data);
        } catch (Throwable t) {
            logger.error("站点 {} 数据拆包发生严重异常，强制清空缓冲区以恢复状态: {}", stationId, t.getMessage(), t);
            state.buffer.clear();
        } finally {
            state.lock.unlock();
        }
    }

    public LocalDate getCurrentZdaDate(String stationId) {
        StationState state = stationStates.get(stationId);
        return (state != null) ? state.cachedZdaDate : null;
    }

    private boolean shouldStoreNmeaFast(String nmea) {
        if (nmea.length() < 6 || nmea.charAt(0) != '$') return false;
        char c1 = nmea.charAt(3);
        char c2 = nmea.charAt(4);
        char c3 = nmea.charAt(5);

        if (c1 == 'G') {
            return (c2 == 'G' && c3 == 'A') || (c2 == 'S' && c3 == 'V');
        }
        return c1 == 'Z' && c2 == 'D' && c3 == 'A';
    }

    public Set<String> getNmeaStorageWhitelist() {
        return new HashSet<>(NMEA_STORAGE_WHITELIST);
    }

    // ==================== 核心处理逻辑 ====================

    private void processBuffer(StationState state, byte[] newData) {
        ByteBuffer buffer = state.buffer;

        if (newData.length > this.maxBufferSize) {
            logger.error("站点 {} 流入数据帧过大 ({} 字节)，直接丢弃", state.stationId, newData.length);
            return;
        }

        if (buffer.position() + newData.length > buffer.capacity()) {
            if (buffer.capacity() < this.maxBufferSize) {
                int newCapacity = Math.min(Math.max(buffer.capacity() * 2, newData.length + buffer.position()), this.maxBufferSize);
                ByteBuffer newBuffer = ByteBuffer.allocate(newCapacity);
                buffer.flip();
                newBuffer.put(buffer);
                state.buffer = newBuffer;
                buffer = newBuffer;
            } else {
                buffer.clear();
                globalBufferOverflowCount.incrementAndGet();
                logger.warn("站点 {} 缓冲区溢出，丢弃旧数据", state.stationId);
            }
        }

        buffer.put(newData);
        buffer.flip();

        while (buffer.hasRemaining()) {
            int pos = buffer.position();
            byte firstByte = buffer.get(pos);

            if (firstByte == NMEA_START) {
                int endIdx = findNmeaEnd(buffer);
                if (endIdx < 0) {
                    buffer.position(pos);
                    buffer.compact();
                    return;
                }

                int len = endIdx - pos + 1;
                byte[] nmeaBytes = new byte[len];
                buffer.get(nmeaBytes);

                String nmea = new String(nmeaBytes, StandardCharsets.UTF_8);
                processNmea(state, nmea);
                nmeaCount.incrementAndGet();

            } else if (firstByte == RTCM3_PREAMBLE) {
                int frameLen = parseRtcmFrameLength(buffer);
                if (frameLen < 0) {
                    buffer.position(pos);
                    buffer.compact();
                    return;
                }

                int totalLen = 3 + frameLen + 3;
                if (buffer.remaining() < totalLen) {
                    buffer.position(pos);
                    buffer.compact();
                    return;
                }

                if (!validateRtcmCrc(buffer, pos, frameLen)) {
                    buffer.position(pos + 1);
                    continue;
                }

                byte[] rtcmData = new byte[totalLen];
                buffer.get(rtcmData);

                processRtcm(state, rtcmData);
                rtcmCount.incrementAndGet();

            } else {
                buffer.get();
            }
        }

        buffer.clear();
    }

    private void processNmea(StationState state, String nmea) {
        if (nmea == null || nmea.isEmpty()) return;
        String trimmed = nmea.trim();

        if (zdaParser.isZdaSentence(trimmed)) {
            processZda(state, trimmed);
        } else if (gsvParser.isGsvSentence(trimmed)) {
            processGsv(state, trimmed);
        } else if (ggaParser.isGgaSentence(trimmed)) {
            GnssSolution solution = ggaParser.parse(trimmed);
            if (solution != null && asyncProcessor != null) {
                asyncProcessor.submitGnssSolution(solution);
            }
        }

        if (asyncProcessor != null) {
            if (shouldStoreNmeaFast(trimmed)) {
                asyncProcessor.submitNmea(state.stationId, trimmed);
                nmeaStorageCount.incrementAndGet();
            } else {
                nmeaFilteredCount.incrementAndGet();
            }
        }
    }

    private void processZda(StationState state, String nmea) {
        ZdaParser.ZdaResult result = zdaParser.parseWithResult(nmea);
        if (result.isSuccess()) {
            state.cachedZdaDate = result.getDate();
            state.cachedZdaTime = result.getTime();
            state.lastZdaTimestamp = System.currentTimeMillis();
            state.zdaReceivedCount++;
            state.dateSource = "ZDA";
            zdaCount.incrementAndGet();

            if (fusionService != null) {
                fusionService.updateZdaDate(state.stationId, state.cachedZdaDate, state.cachedZdaTime, state.lastZdaTimestamp);
            }
        }
    }

    private void processGsv(StationState state, String nmea) {
        List<GsvSatelliteData> satellites = gsvParser.parse(nmea);
        if (satellites == null || satellites.isEmpty()) return;

        gsvCount.incrementAndGet();

        LocalDate obsDate = state.cachedZdaDate != null ? state.cachedZdaDate : LocalDate.now();
        String dateSource = state.cachedZdaDate != null ? state.dateSource : "SYSTEM";

        if (fusionService != null) {
            fusionService.processGsvData(state.stationId, satellites, state.currentEpochTime, obsDate, dateSource);
        }
    }

    /**
     * 处理 RTCM3 二进制原始数据流
     */
    private void processRtcm(StationState state, byte[] rtcmData) {
        Pointer ctx = contextManager.getOrCreateContext(state.stationId);
        if (ctx == null) return;

        try {
            // 【核心修复2】：传入 state，复用底层 JNA 结构体缓存，消除 GC 性能瓶颈
            RtklibNative.JavaObs[] obs = parseRtcmWithNative(ctx, rtcmData, state);
            if (obs != null && obs.length > 0) {
                long realGnssEpochTime = extractEpochTimeFromObs(obs[0]);
                state.currentEpochTime = realGnssEpochTime;

                LocalDate obsDate = state.cachedZdaDate != null ? state.cachedZdaDate : LocalDate.now();
                String dateSource = state.cachedZdaDate != null ? state.dateSource : "SYSTEM";

                if (fusionService != null) {
                    fusionService.processRtcmData(state.stationId, obs, realGnssEpochTime, obsDate, dateSource);
                }
            }
        } catch (Throwable t) {
            logger.error("站点 {} RTCM解析(Native)发生致命异常: {}", state.stationId, t.getMessage());
        }
        // 🚨🚨🚨 【核心修复1】：彻底删除 finally 中的 releaseContext，保证 RTKLIB 卡尔曼滤波状态机跨历元存活！🚨🚨🚨

        if (asyncProcessor != null) {
            asyncProcessor.submitRtcm(state.stationId, rtcmData);
        }
    }

    // ==================== 辅助方法 ====================

    private long extractEpochTimeFromObs(RtklibNative.JavaObs obs) {
        if (obs.time != 0) {
            return obs.time * 1000L + Math.round(obs.sec * 1000.0);
        }
        throw new IllegalStateException("无法从RTCM观测值提取高精度GNSS历元时间");
    }

    private StationState getOrCreateStationState(String stationId) {
        return stationStates.computeIfAbsent(stationId, id -> {
            StationState state = new StationState();
            state.stationId = id;
            state.buffer = ByteBuffer.allocate(bufferSize > 0 ? bufferSize : DEFAULT_BUFFER_SIZE);
            state.lock = new ReentrantLock();
            state.currentEpochTime = System.currentTimeMillis();
            state.dateSource = "NONE";

            // 【核心修复2】：在状态初始化时，分配唯一的 JNA 缓存对象，避免高频 GC
            state.cachedObsRef = new RtklibNative.JavaObs.ByReference();
            state.cachedObsArray = (RtklibNative.JavaObs[]) state.cachedObsRef.toArray(this.maxObs);

            return state;
        });
    }

    private int findNmeaEnd(ByteBuffer buffer) {
        int pos = buffer.position();
        int limit = buffer.limit();
        for (int i = pos; i < limit; i++) {
            if (buffer.get(i) == '\n') return i;
            if (i < limit - 1 && buffer.get(i) == '\r' && buffer.get(i + 1) == '\n') return i + 1;
        }
        return -1;
    }

    private int parseRtcmFrameLength(ByteBuffer buffer) {
        if (buffer.remaining() < 3) return -1;
        int pos = buffer.position();
        if (buffer.get(pos) != RTCM3_PREAMBLE) return -1;
        return ((buffer.get(pos + 1) & 0x03) << 8) | (buffer.get(pos + 2) & 0xFF);
    }

    private boolean validateRtcmCrc(ByteBuffer buffer, int pos, int frameLen) {
        int crc = 0;
        int dataLen = 3 + frameLen;
        for (int i = 0; i < dataLen; i++) {
            crc = ((crc << 8) & 0xFFFFFF) ^ CRC24Q_TABLE[(crc >>> 16) ^ (buffer.get(pos + i) & 0xFF)];
        }
        int expectedCrc = ((buffer.get(pos + dataLen) & 0xFF) << 16) |
                ((buffer.get(pos + dataLen + 1) & 0xFF) << 8) |
                (buffer.get(pos + dataLen + 2) & 0xFF);
        return crc == expectedCrc;
    }

    /**
     * 引入享元模式（复用 state 中的 cached 结构体）
     */
    private RtklibNative.JavaObs[] parseRtcmWithNative(Pointer ctx, byte[] rtcmData, StationState state) {
        // 直接使用绑定的预分配对象，拒绝 new 对象！
        RtklibNative.JavaObs.ByReference obsRef = state.cachedObsRef;
        RtklibNative.JavaObs[] obsArray = state.cachedObsArray;

        int count = RtklibNative.INSTANCE.rtklib_parse_rtcm_frame_ex(ctx, rtcmData, rtcmData.length, obsRef, this.maxObs);
        if (count <= 0) return null;

        RtklibNative.JavaObs[] result = new RtklibNative.JavaObs[count];
        for (int i = 0; i < count; i++) {
            obsArray[i].read(); // 将 Native 内存同步映射到现存的 Java 对象上
            result[i] = obsArray[i];
        }
        return result;
    }

    private String truncateForLog(String s) {
        if (s == null) return "null";
        return s.length() > 80 ? s.substring(0, 80) + "..." : s;
    }

    // ==================== 统计方法 ====================

    public String getStatistics() {
        return String.format(
                "NMEA统计: 总接收=%d, 入库=%d, 过滤=%d, GSV=%d, ZDA=%d, RTCM=%d, 缓冲溢出=%d",
                nmeaCount.get(), nmeaStorageCount.get(), nmeaFilteredCount.get(),
                gsvCount.get(), zdaCount.get(), rtcmCount.get(), globalBufferOverflowCount.get()
        );
    }

    public long getNmeaStorageCount() { return nmeaStorageCount.get(); }
    public long getNmeaFilteredCount() { return nmeaFilteredCount.get(); }

    public void resetStatistics() {
        nmeaCount.set(0);
        nmeaStorageCount.set(0);
        nmeaFilteredCount.set(0);
        gsvCount.set(0);
        zdaCount.set(0);
        rtcmCount.set(0);
        globalBufferOverflowCount.set(0);
    }

    // ==================== 内部类 ====================

    private static class StationState {
        String stationId;
        ByteBuffer buffer;
        ReentrantLock lock;
        Long currentEpochTime;
        LocalDate cachedZdaDate;
        LocalTime cachedZdaTime;
        long lastZdaTimestamp;
        long zdaReceivedCount;
        String dateSource;

        RtklibNative.JavaObs.ByReference cachedObsRef;
        RtklibNative.JavaObs[] cachedObsArray;
    }
}
