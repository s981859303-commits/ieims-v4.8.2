package com.ruoyi.gnss.service;

import com.ruoyi.gnss.domain.GnssSolution;
import com.ruoyi.gnss.domain.NmeaRecord;
import com.ruoyi.gnss.service.impl.SatelliteDataFusionService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
        import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 混合日志解析器
 * 功能：
 * 1. 解析 NMEA 和 RTCM 混合数据流
 * 2. 调用 RtklibNative 解析 RTCM 观测数据
 * 3. 解析 GSV 语句获取卫星仰角、方位角、信噪比
 * 4. 融合 GSV 和 RTCM 数据
 * 5. 将融合后的数据写入 TDengine
 * <p>
 * 优化内容：
 * 1. 支持多站点处理
 * 2. 优化锁粒度
 * 3. 减少对象分配
 * 4. 支持站点上下文传递
 * </p>
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

    // ==================== 缓冲区和锁 ====================

    private ByteBuffer buffer;
    private final ReentrantLock bufferLock = new ReentrantLock();

    // ==================== 时间相关 ====================

    private volatile Long currentEpochTime = null;
    private final AtomicLong lastPrintTime = new AtomicLong(System.currentTimeMillis());

    // ==================== 统计信息 ====================

    private final AtomicLong bufferOverflowCount = new AtomicLong(0);
    private final AtomicLong nmeaCount = new AtomicLong(0);
    private final AtomicLong rtcmCount = new AtomicLong(0);

    // ==================== 缓存的字符串常量 ====================

    private static final String PREFIX_GNGGA = "$GNGGA";
    private static final String PREFIX_GPGGA = "$GPGGA";
    private static final String PREFIX_BDGGA = "$BDGGA";

    // ==================== 初始化 ====================

    @PostConstruct
    public void init() {
        this.buffer = ByteBuffer.allocate(bufferSize);

        // 设置默认站点ID
        StationContext.setDefaultStationId(defaultStationId);

        logger.info("MixedLogSplitter 初始化完成（多站点优化版），缓冲区大小: {} bytes, 默认站点: {}",
                bufferSize, defaultStationId);

        if (rtcmStorageService == null) {
            logger.warn("警告：rtcmStorageService 未注入");
        }
        if (gsvParser == null) {
            logger.warn("警告：gsvParser 未注入，GSV 解析功能将不可用");
        }
        if (fusionService == null) {
            logger.warn("警告：fusionService 未注入，数据融合功能将不可用");
        }
        if (asyncProcessor == null) {
            logger.warn("警告：asyncProcessor 未注入，将使用同步模式");
        }
    }

    // ==================== 入口方法 ====================

    /**
     * 接收数据入口（使用默认站点）
     */
    public void pushData(byte[] newBytes) {
        pushData(defaultStationId, newBytes);
    }

    /**
     * 接收数据入口（指定站点）
     *
     * @param stationId 站点ID
     * @param newBytes  原始字节数据
     */
    public void pushData(String stationId, byte[] newBytes) {
        if (newBytes == null || newBytes.length == 0) return;

        bufferLock.lock();
        try {
            // 改进的缓冲区管理
            if (!tryEnsureCapacity(newBytes.length)) {
                long overflowCount = bufferOverflowCount.incrementAndGet();
                if (overflowCount % 100 == 1) {
                    logger.warn("缓冲区溢出，丢弃数据包，长度: {} bytes，累计溢出: {} 次，站点: {}",
                            newBytes.length, overflowCount, stationId);
                }
                return;
            }

            buffer.put(newBytes);
            buffer.flip();

            // 在站点上下文中解析
            try (StationContext.Scope scope = StationContext.withStation(stationId)) {
                parseAndDispatch(stationId);
            }

            buffer.compact();

        } catch (Exception e) {
            logger.error("数据接收缓冲区异常: {}", e.getMessage());
            if (buffer != null) {
                buffer.clear();
            }
        } finally {
            bufferLock.unlock();
        }
    }

    /**
     * 尝试确保缓冲区有足够容量
     */
    private boolean tryEnsureCapacity(int required) {
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

    private void parseAndDispatch(String stationId) {
        while (buffer.hasRemaining()) {
            buffer.mark();
            byte b = buffer.get();

            if (b == 0x24) { // NMEA ($)
                if (!tryExtractNmea(stationId)) {
                    buffer.reset();
                    break;
                }
            } else if ((b & 0xFF) == 0xD3) { // RTCM (0xD3)
                if (!tryExtractRtcm(stationId)) {
                    buffer.reset();
                    break;
                }
            }
        }
    }

    private boolean tryExtractNmea(String stationId) {
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
                notifyNmea(stationId, nmea);

                buffer.position(i + 1);
                return true;
            }
        }
        return false;
    }

    private boolean tryExtractRtcm(String stationId) {
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

        notifyRtcm(stationId, rtcmFrame);
        return true;
    }

    // ==================== NMEA 处理 ====================

    private void notifyNmea(String stationId, String nmea) {
        nmeaCount.incrementAndGet();

        // 异步存储原始 NMEA 数据
        if (asyncProcessor != null) {
            asyncProcessor.submitNmea(stationId, nmea);
        } else if (nmeaStorageService != null) {
            // 降级：同步存储
            NmeaRecord record = new NmeaRecord(new Date(), nmea);
            nmeaStorageService.saveNmeaData(stationId, record);
        }

        // 处理 GGA 语句（提取历元时间）
        if (isGgaSentence(nmea)) {
            processGgaData(stationId, nmea);
        }

        // 处理 GSV 语句
        if (fusionService != null && gsvParser != null && gsvParser.isGsvSentence(nmea)) {
            fusionService.processGsvData(stationId, nmea);
        }

        // 通知监听器
        if (listeners != null) {
            for (GnssDataListener listener : listeners) {
                try {
                    listener.onNmeaReceived(nmea);
                } catch (Exception ignored) {
                }
            }
        }
    }

    private boolean isGgaSentence(String nmea) {
        return nmea.startsWith(PREFIX_GNGGA) ||
                nmea.startsWith(PREFIX_GPGGA) ||
                nmea.startsWith(PREFIX_BDGGA);
    }

    private void processGgaData(String stationId, String ggaLine) {
        try {
            String[] parts = ggaLine.split(",", -1);

            if (parts.length < 10) {
                return;
            }

            if (parts[2] == null || parts[2].isEmpty() ||
                    parts[4] == null || parts[4].isEmpty()) {
                return;
            }

            // 提取 UTC 时间作为历元时间
            if (parts.length > 1 && parts[1] != null && !parts[1].isEmpty()) {
                currentEpochTime = parseUtcTime(parts[1]);
                if (fusionService != null) {
                    fusionService.setEpochTime(currentEpochTime);
                }
            }

            double lat = nmeaToDecimal(parts[2], parts[3]);
            double lon = nmeaToDecimal(parts[4], parts[5]);

            int satUsed = safeParseInt(parts[7], 0);
            double hdop = safeParseDouble(parts[8], 0.0);
            double alt = safeParseDouble(parts[9], 0.0);
            int status = safeParseInt(parts[6], 0);

            // 异步存储 GNSS 解算结果
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
        if (utcTime == null || utcTime.length() < 6) {
            return null;
        }

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

    // ==================== 安全解析方法 ====================

    private int safeParseInt(String value, int defaultValue) {
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    private double safeParseDouble(String value, double defaultValue) {
        if (value == null || value.isEmpty()) {
            return defaultValue;
        }
        try {
            return Double.parseDouble(value);
        } catch (NumberFormatException e) {
            return defaultValue;
        }
    }

    // ==================== RTCM 处理 ====================

    private void notifyRtcm(String stationId, byte[] data) {
        rtcmCount.incrementAndGet();

        // 异步存储原始 RTCM 数据
        if (asyncProcessor != null) {
            asyncProcessor.submitRtcm(stationId, data);
        } else if (rtcmStorageService != null) {
            // 降级：同步存储
            rtcmStorageService.saveRtcmRawData(data);
        }

        // 解算 RTCM 数据
        decodeRtcmData(stationId, data);

        // 通知监听器
        if (listeners != null) {
            for (GnssDataListener listener : listeners) {
                try {
                    listener.onRtcmReceived(data);
                } catch (Exception ignored) {
                }
            }
        }
    }

    private void decodeRtcmData(String stationId, byte[] rtcmData) {
        try {
            RtklibNative.JavaObs.ByReference obsRef = new RtklibNative.JavaObs.ByReference();
            RtklibNative.JavaObs[] obsArray = (RtklibNative.JavaObs[]) obsRef.toArray(64);

            int count = RtklibNative.INSTANCE.parse_rtcm_frame(rtcmData, rtcmData.length, obsRef, 64);

            if (count > 0) {
                updateSatCache(stationId, obsArray, count, rtcmData);
            }
        } catch (UnsatisfiedLinkError e) {
            logger.error("找不到 rtklib_bridge.dll，请检查系统路径！");
        } catch (Exception e) {
            logger.error("RTCM 解码异常: {}", e.getMessage());
        }
    }

    private void updateSatCache(String stationId, RtklibNative.JavaObs[] newObs, int count, byte[] rtcmData) {
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
            Double snr = obs.snr[0] > 0 ? obs.snr[0] * 4.0 : null;

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

        // 定时触发融合入库
        long now = System.currentTimeMillis();
        long lastTime = lastPrintTime.get();
        if (now - lastTime > printIntervalMs) {
            if (lastPrintTime.compareAndSet(lastTime, now)) {
                fuseAndStoreData(stationId);
            }
        }
    }

    private int getRtcmMessageType(byte[] rtcmData) {
        if (rtcmData == null || rtcmData.length < 6) {
            return 0;
        }
        return ((rtcmData[3] & 0xFF) << 4) | ((rtcmData[4] & 0xF0) >> 4);
    }

    private void fuseAndStoreData(String stationId) {
        if (fusionService != null) {
            fusionService.fuseAndStore(stationId);
        }
    }

    // ==================== 公共接口 ====================

    public Long getCurrentEpochTime() {
        return currentEpochTime;
    }

    public long getBufferOverflowCount() {
        return bufferOverflowCount.get();
    }

    public long getNmeaCount() {
        return nmeaCount.get();
    }

    public long getRtcmCount() {
        return rtcmCount.get();
    }

    public String getStatistics() {
        if (asyncProcessor != null) {
            return asyncProcessor.getStatistics();
        }
        return String.format("NMEA=%d, RTCM=%d, Overflow=%d",
                nmeaCount.get(), rtcmCount.get(), bufferOverflowCount.get());
    }
}
