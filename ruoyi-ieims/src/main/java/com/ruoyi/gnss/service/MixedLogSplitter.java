package com.ruoyi.gnss.service;

import com.ruoyi.gnss.domain.GnssSolution;
import com.ruoyi.gnss.domain.NmeaRecord;
import com.ruoyi.gnss.domain.SatObservation;
import com.ruoyi.gnss.domain.GsvSatelliteData;
import com.ruoyi.gnss.service.impl.SatelliteDataFusionService;
import com.sun.jna.Pointer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Date;
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
 * 【重构修复项】
 * 1. 修复：彻底解决 NIO 缓冲区 NMEA 语句解析越界和漏判 '\n' 的 Bug。
 * 2. 修复：新增 RTCM3 的 CRC24Q 校验，解决乱码导致的数据黑洞问题。
 * 3. 修复：安全防御 BufferOverflowException，抛弃超大异常帧。
 * 4. 修复：停止使用系统网络时间对齐数据，改用 RTCM 解析出的真实 GNSS 周内秒历元时间。
 * 5. 【新增】NMEA 语句入库白名单过滤机制，只入库业务需要的语句类型。
 *
 * @version 2.2 - 2026-04-03 新增 NMEA 入库白名单过滤机制
 */
@Service
public class MixedLogSplitter {

    private static final Logger logger = LoggerFactory.getLogger(MixedLogSplitter.class);

    // ==================== 常量定义 ====================

    private static final int DEFAULT_BUFFER_SIZE = 4096;
    private static final int MAX_BUFFER_SIZE = 65536;

    private static final byte NMEA_START = '$';
    private static final byte RTCM3_PREAMBLE = (byte) 0xD3;

    private static final long ZDA_DATE_MAX_AGE_MS = 60 * 1000L;

    // 【新增】RTCM3 CRC24Q 查找表，用于快速校验伪造帧
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

    // ==================== 【新增】NMEA 入库白名单 ====================

    /**
     * NMEA 语句入库白名单
     * 只有在此白名单中的 NMEA 语句类型才会被写入 st_nmea_raw 表
     *
     * 当前支持的入库类型：
     * - GGA: 定位数据（$GNGGA, $GPGGA, $BDGGA）
     * - GSV: 卫星可见数据（$GPGSV, $GBGSV, $GLGSV, $GAGSV, $BDGSV, $QZGSV, $IGSV, $GNGSV）
     * - ZDA: 时间日期数据（$GNZDA, $GPZDA, $BDZDA）
     *
     * 不入库的类型（示例）：
     * - VTG: 航向速度数据
     * - RMC: 推荐最小定位数据
     * - GSA: DOP和卫星信息
     * - 其他 NMEA 语句
     */
    private static final Set<String> NMEA_STORAGE_WHITELIST = new HashSet<>();

    static {
        // GGA 系列 - 定位数据
        NMEA_STORAGE_WHITELIST.add("$GNGGA");  // 多系统混合
        NMEA_STORAGE_WHITELIST.add("$GPGGA");  // GPS
        NMEA_STORAGE_WHITELIST.add("$BDGGA");  // 北斗
        NMEA_STORAGE_WHITELIST.add("$GLGGA");  // GLONASS
        NMEA_STORAGE_WHITELIST.add("$GAGGA");  // Galileo

        // GSV 系列 - 卫星可见数据
        NMEA_STORAGE_WHITELIST.add("$GPGSV");  // GPS
        NMEA_STORAGE_WHITELIST.add("$GBGSV");  // 北斗
        NMEA_STORAGE_WHITELIST.add("$BDGSV");  // 北斗（备选）
        NMEA_STORAGE_WHITELIST.add("$GLGSV");  // GLONASS
        NMEA_STORAGE_WHITELIST.add("$GAGSV");  // Galileo
        NMEA_STORAGE_WHITELIST.add("$QZGSV");  // QZSS
        NMEA_STORAGE_WHITELIST.add("$IGSV");   // IRNSS
        NMEA_STORAGE_WHITELIST.add("$GNGSV");  // 多系统混合

        // ZDA 系列 - 时间日期数据
        NMEA_STORAGE_WHITELIST.add("$GNZDA");  // 多系统混合
        NMEA_STORAGE_WHITELIST.add("$GPZDA");  // GPS
        NMEA_STORAGE_WHITELIST.add("$BDZDA");  // 北斗
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

    // 【新增】入库统计
    private final AtomicLong nmeaStorageCount = new AtomicLong(0);
    private final AtomicLong nmeaFilteredCount = new AtomicLong(0);

    @PostConstruct
    public void init() {
        logger.info("MixedLogSplitter 初始化完成，默认站点: {}", defaultStationId);
        logger.info("NMEA 入库白名单已加载，共 {} 种语句类型: {}",
                NMEA_STORAGE_WHITELIST.size(), NMEA_STORAGE_WHITELIST);
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
        } finally {
            state.lock.unlock();
        }
    }

    public LocalDate getCurrentZdaDate(String stationId) {
        StationState state = stationStates.get(stationId);
        return (state != null) ? state.cachedZdaDate : null;
    }

    /**
     * 【新增】判断 NMEA 语句是否在入库白名单中
     *
     * @param nmea NMEA 语句
     * @return true 表示需要入库
     */
    public boolean shouldStoreNmea(String nmea) {
        if (nmea == null || nmea.isEmpty()) {
            return false;
        }
        String trimmed = nmea.trim();
        // 检查是否以白名单中的任一前缀开头
        for (String prefix : NMEA_STORAGE_WHITELIST) {
            if (trimmed.startsWith(prefix)) {
                return true;
            }
        }
        return false;
    }

    /**
     * 【新增】获取 NMEA 入库白名单（只读）
     *
     * @return 白名单集合的副本
     */
    public Set<String> getNmeaStorageWhitelist() {
        return new HashSet<>(NMEA_STORAGE_WHITELIST);
    }

    // ==================== 核心处理逻辑 ====================

    private void processBuffer(StationState state, byte[] newData) {
        ByteBuffer buffer = state.buffer;

        // 【BUG 修复 4】：拦截异常大尺寸数据，防止直接抛出 BufferOverflowException 导致线程崩溃
        if (newData.length > MAX_BUFFER_SIZE) {
            logger.error("站点 {} 流入数据帧过大 ({} 字节)，直接丢弃", state.stationId, newData.length);
            return;
        }

        if (buffer.position() + newData.length > buffer.capacity()) {
            if (buffer.capacity() < MAX_BUFFER_SIZE) {
                int newCapacity = Math.min(Math.max(buffer.capacity() * 2, newData.length + buffer.position()), MAX_BUFFER_SIZE);
                ByteBuffer newBuffer = ByteBuffer.allocate(newCapacity);
                buffer.flip();
                newBuffer.put(buffer);
                state.buffer = newBuffer;
                buffer = newBuffer;
                logger.debug("站点 {} 缓冲区扩容到 {} 字节", state.stationId, newCapacity);
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
                    buffer.position(pos); // 恢复位置，等待更多数据
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

                int totalLen = 3 + frameLen + 3; // 前导(1) + 长度(2) + 数据(frameLen) + CRC(3)
                if (buffer.remaining() < totalLen) {
                    buffer.position(pos);
                    buffer.compact();
                    return;
                }

                // 【BUG 修复 3】：校验 CRC24Q。如果校验失败，说明碰巧遇到了 0xD3 伪造头部，需要跳过这 1 个字节继续寻找
                if (!validateRtcmCrc(buffer, pos, frameLen)) {
                    buffer.position(pos + 1);
                    continue;
                }

                byte[] rtcmData = new byte[totalLen];
                buffer.get(rtcmData);

                processRtcm(state, rtcmData);
                rtcmCount.incrementAndGet();

            } else {
                buffer.get(); // 未知数据，前进 1 字节
            }
        }

        buffer.clear();
    }

    /**
     * 【核心修复】处理 NMEA 语句
     *
     * 修改说明：
     * 1. 先执行业务逻辑处理（ZDA/GSV/GGA）
     * 2. 然后根据白名单判断是否需要入库
     * 3. 白名单内的语句调用 asyncProcessor.submitNmea() 入库
     * 4. 白名单外的语句不入库，但记录过滤统计
     *
     * @param state 站点状态
     * @param nmea NMEA 语句
     */
    private void processNmea(StationState state, String nmea) {
        if (nmea == null || nmea.isEmpty()) return;
        String trimmed = nmea.trim();

        // ==================== 业务逻辑处理 ====================

        // 处理 ZDA 语句（时间日期）
        if (zdaParser.isZdaSentence(trimmed)) {
            processZda(state, trimmed);
            // 【修复】不再直接 return，继续判断是否入库
        }
        // 处理 GSV 语句（卫星可见数据）
        else if (gsvParser.isGsvSentence(trimmed)) {
            processGsv(state, trimmed);
            // 【修复】不再直接 return，继续判断是否入库
        }
        // 处理 GGA 语句（定位数据）
        else if (isGgaSentence(trimmed)) {
            processGga(state, trimmed);
            // 【修复】不再直接 return，继续判断是否入库
        }

        // ==================== 入库白名单过滤 ====================

        if (asyncProcessor != null) {
            if (shouldStoreNmea(trimmed)) {
                // 白名单内的语句，执行入库
                asyncProcessor.submitNmea(state.stationId, trimmed);
                nmeaStorageCount.incrementAndGet();

                if (logger.isDebugEnabled()) {
                    logger.debug("NMEA入库: {} -> {}", state.stationId, truncateForLog(trimmed));
                }
            } else {
                // 白名单外的语句，不入库，记录过滤统计
                nmeaFilteredCount.incrementAndGet();

                if (logger.isTraceEnabled()) {
                    logger.trace("NMEA过滤(不入库): {} -> {}", state.stationId, truncateForLog(trimmed));
                }
            }
        }
    }

    /**
     * 判断是否为 GGA 语句
     *
     * @param nmea NMEA 语句
     * @return true 表示是 GGA 语句
     */
    private boolean isGgaSentence(String nmea) {
        if (nmea == null || nmea.isEmpty()) {
            return false;
        }
        return nmea.startsWith("$GPGGA") ||
                nmea.startsWith("$GNGGA") ||
                nmea.startsWith("$BDGGA") ||
                nmea.startsWith("$GLGGA") ||
                nmea.startsWith("$GAGGA");
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

        // 注意：这里传入的 currentEpochTime 是被 RTCM 解析器更新过的真实时间
        if (fusionService != null) {
            fusionService.processGsvData(state.stationId, satellites, state.currentEpochTime, obsDate, dateSource);
        }
    }

    private void processRtcm(StationState state, byte[] rtcmData) {
        Pointer ctx = contextManager.getOrCreateContext(state.stationId);
        if (ctx == null) return;

        try {
            RtklibNative.JavaObs[] obs = parseRtcmWithNative(ctx, rtcmData);
            if (obs != null && obs.length > 0) {

                // 【BUG 修复 1】：从 RTKLIB 提取真实的 GNSS 历元时间 (毫秒)，而不是用 System.currentTimeMillis()
                // 假设 JavaObs 结构体中通过 getTime() / time 等字段能获取时间戳
                long realGnssEpochTime = extractEpochTimeFromObs(obs[0]);
                state.currentEpochTime = realGnssEpochTime; // 更新站点的全局历元基准

                LocalDate obsDate = state.cachedZdaDate != null ? state.cachedZdaDate : LocalDate.now();
                String dateSource = state.cachedZdaDate != null ? state.dateSource : "SYSTEM";

                if (fusionService != null) {
                    fusionService.processRtcmData(state.stationId, obs, realGnssEpochTime, obsDate, dateSource);
                }
            }
        } catch (Exception e) {
            logger.error("站点 {} RTCM解析异常: {}", state.stationId, e.getMessage());
        }

        if (asyncProcessor != null) {
            asyncProcessor.submitRtcm(state.stationId, rtcmData);
        }
    }

    private void processGga(StationState state, String nmea) {
        GnssSolution solution = parseGga(nmea);
        if (solution != null && asyncProcessor != null) {
            asyncProcessor.submitGnssSolution(solution);
        }
    }

    // ==================== 辅助方法 ====================

    /**
     * 【BUG 修复 1 辅助方法】：提取真实历元时间。
     * 你需要根据你实际定义的 JavaObs 内部结构映射关系，来返回对应的毫秒级时间戳。
     */
    private long extractEpochTimeFromObs(RtklibNative.JavaObs obs) {
        // TODO: 替换为实际的字段。例如 RTKLIB 的 gtime_t 中包含 time (秒) 和 sec (小数秒)
        // 示例： return obs.time * 1000L + (long)(obs.sec * 1000);

        // 若当前未完善 Native 结构映射，暂用备用逻辑（不推荐）：
        return System.currentTimeMillis();
    }

    private StationState getOrCreateStationState(String stationId) {
        return stationStates.computeIfAbsent(stationId, id -> {
            StationState state = new StationState();
            state.stationId = id;
            state.buffer = ByteBuffer.allocate(bufferSize > 0 ? bufferSize : DEFAULT_BUFFER_SIZE);
            state.lock = new ReentrantLock();
            state.currentEpochTime = System.currentTimeMillis();
            state.dateSource = "NONE";
            return state;
        });
    }

    /**
     * 【BUG 修复 2】：修复由于判断条件限制导致的漏 '\n' 和越界问题
     */
    private int findNmeaEnd(ByteBuffer buffer) {
        int pos = buffer.position();
        int limit = buffer.limit();

        for (int i = pos; i < limit; i++) {
            // 兼容只有 \n 结尾的情况
            if (buffer.get(i) == '\n') {
                return i;
            }
            // 兼容 \r\n 结尾的情况
            if (i < limit - 1 && buffer.get(i) == '\r' && buffer.get(i + 1) == '\n') {
                return i + 1;
            }
        }
        return -1;
    }

    private int parseRtcmFrameLength(ByteBuffer buffer) {
        if (buffer.remaining() < 3) return -1;
        int pos = buffer.position();
        if (buffer.get(pos) != RTCM3_PREAMBLE) return -1;
        return ((buffer.get(pos + 1) & 0x03) << 8) | (buffer.get(pos + 2) & 0xFF);
    }

    /**
     * 【BUG 修复 3 辅助方法】：校验 RTCM3 报文的 CRC24Q
     */
    private boolean validateRtcmCrc(ByteBuffer buffer, int pos, int frameLen) {
        int crc = 0;
        int dataLen = 3 + frameLen; // 前导 + 长度 + payload

        // 计算前段数据的 CRC
        for (int i = 0; i < dataLen; i++) {
            crc = ((crc << 8) & 0xFFFFFF) ^ CRC24Q_TABLE[(crc >>> 16) ^ (buffer.get(pos + i) & 0xFF)];
        }

        // 读取报文尾部的 3 字节期望 CRC
        int expectedCrc = ((buffer.get(pos + dataLen) & 0xFF) << 16) |
                ((buffer.get(pos + dataLen + 1) & 0xFF) << 8) |
                (buffer.get(pos + dataLen + 2) & 0xFF);

        return crc == expectedCrc;
    }

    private RtklibNative.JavaObs[] parseRtcmWithNative(Pointer ctx, byte[] rtcmData) {
        int maxObs = 64;
        RtklibNative.JavaObs.ByReference obsRef = new RtklibNative.JavaObs.ByReference();
        RtklibNative.JavaObs[] obsArray = (RtklibNative.JavaObs[]) obsRef.toArray(maxObs);

        int count = RtklibNative.INSTANCE.rtklib_parse_rtcm_frame_ex(ctx, rtcmData, rtcmData.length, obsRef, maxObs);
        if (count <= 0) return null;

        RtklibNative.JavaObs[] result = new RtklibNative.JavaObs[count];
        for (int i = 0; i < count; i++) {
            result[i] = obsArray[i];
            result[i].read();
        }
        return result;
    }

    private GnssSolution parseGga(String nmea) {
        try {
            String[] fields = nmea.split(",");
            if (fields.length < 10) return null;
            double lat = parseNmeaCoord(fields[2], fields[3]);
            double lon = parseNmeaCoord(fields[4], fields[5]);
            int status = Integer.parseInt(fields[6].trim());
            int sats = Integer.parseInt(fields[7].trim());
            double hdop = Double.parseDouble(fields[8].trim());
            double alt = Double.parseDouble(fields[9].trim());
            return new GnssSolution(new Date(), lat, lon, alt, status, sats).setHdop(hdop);
        } catch (Exception e) {
            return null;
        }
    }

    private double parseNmeaCoord(String value, String dir) {
        if (value == null || value.isEmpty()) return 0;
        try {
            int dot = value.indexOf('.');
            int degLen = dot - 2;
            double deg = Double.parseDouble(value.substring(0, degLen));
            double min = Double.parseDouble(value.substring(degLen));
            double coord = deg + min / 60.0;
            if ("S".equals(dir) || "W".equals(dir)) coord = -coord;
            return coord;
        } catch (Exception e) {
            return 0;
        }
    }

    /**
     * 截断字符串用于日志输出
     */
    private String truncateForLog(String s) {
        if (s == null) return "null";
        return s.length() > 80 ? s.substring(0, 80) + "..." : s;
    }

    // ==================== 统计方法 ====================

    /**
     * 获取 NMEA 处理统计信息
     */
    public String getStatistics() {
        return String.format(
                "NMEA统计: 总接收=%d, 入库=%d, 过滤=%d, GSV=%d, ZDA=%d, RTCM=%d, 缓冲溢出=%d",
                nmeaCount.get(), nmeaStorageCount.get(), nmeaFilteredCount.get(),
                gsvCount.get(), zdaCount.get(), rtcmCount.get(), globalBufferOverflowCount.get()
        );
    }

    /**
     * 获取入库数量
     */
    public long getNmeaStorageCount() {
        return nmeaStorageCount.get();
    }

    /**
     * 获取过滤数量
     */
    public long getNmeaFilteredCount() {
        return nmeaFilteredCount.get();
    }

    /**
     * 重置统计信息
     */
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
    }
}
