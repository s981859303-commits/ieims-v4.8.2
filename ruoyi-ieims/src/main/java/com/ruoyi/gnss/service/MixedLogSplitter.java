package com.ruoyi.gnss.service;

import com.ruoyi.gnss.domain.GnssSolution;
import com.ruoyi.gnss.domain.NmeaRecord;
import com.ruoyi.gnss.service.impl.SatelliteDataFusionService;
import com.ruoyi.gnss.service.impl.TDengineRtcmStorageServiceImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

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
 * 新增功能：
 * - GPGSV/GBGSV 语句解析
 * - 卫星编号统一转换
 * - GSV + RTCM 数据融合
 * - 卫星观测数据入库
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

    // ==================== 依赖注入 ====================

    @Autowired(required = false)
    private List<GnssDataListener> listeners = new ArrayList<>();

    @Autowired(required = false)
    private IGnssStorageService gnssStorageService;

    @Autowired(required = false)
    private INmeaStorageService nmeaStorageService;

    @Autowired(required = false)
    private TDengineRtcmStorageServiceImpl rtcmStorageService;

    @Autowired(required = false)
    private GsvParser gsvParser;

    @Autowired(required = false)
    private SatelliteDataFusionService fusionService;

    // ==================== 缓冲区和锁 ====================

    private ByteBuffer buffer;
    private final ReentrantLock bufferLock = new ReentrantLock();

    // ==================== 时间相关 ====================

    /** 当前历元时间（从 GNGGA 解析） */
    private volatile Long currentEpochTime = null;

    /** 最后打印时间 */
    private final AtomicLong lastPrintTime = new AtomicLong(System.currentTimeMillis());

    // ==================== 缓存的字符串常量 ====================

    private static final String PREFIX_GNGGA = "$GNGGA";
    private static final String PREFIX_GPGGA = "$GPGGA";
    private static final String PREFIX_BDGGA = "$BDGGA";

    // ==================== 统计信息 ====================

    /** 缓冲区溢出计数 */
    private final AtomicLong bufferOverflowCount = new AtomicLong(0);

    // ==================== 初始化 ====================

    @javax.annotation.PostConstruct
    public void init() {
        this.buffer = ByteBuffer.allocate(bufferSize);
        logger.info("MixedLogSplitter 初始化完成（修复版），缓冲区大小: {} bytes", bufferSize);

        if (rtcmStorageService == null) {
            logger.error("警告：rtcmStorageService 未注入！请检查 tdengine.enabled 配置！");
        }
        if (gsvParser == null) {
            logger.warn("警告：gsvParser 未注入，GSV 解析功能将不可用");
        }
        if (fusionService == null) {
            logger.warn("警告：fusionService 未注入，数据融合功能将不可用");
        }
    }

    // ==================== 入口方法 ====================

    public void pushData(byte[] newBytes) {
        if (newBytes == null || newBytes.length == 0) return;

        bufferLock.lock();
        try {
            // 修复：改进缓冲区满时的处理逻辑
            if (!tryEnsureCapacity(newBytes.length)) {
                // 无法容纳新数据，记录警告并丢弃
                long overflowCount = bufferOverflowCount.incrementAndGet();
                if (overflowCount % 100 == 1) {  // 每 100 次只打印一次
                    logger.warn("缓冲区溢出，丢弃数据包，长度: {} bytes，累计溢出: {} 次",
                            newBytes.length, overflowCount);
                }
                return;
            }

            buffer.put(newBytes);
            buffer.flip();
            parseAndDispatch();
            buffer.compact();

        } catch (Exception e) {
            logger.error("数据接收缓冲区异常: {}", e.getMessage());
            // 异常情况下才清空缓冲区
            if (buffer != null) {
                buffer.clear();
            }
        } finally {
            bufferLock.unlock();
        }
    }

    /**
     * 尝试确保缓冲区有足够容量
     *
     * 修复：不再直接清空缓冲区，而是尝试压缩或拒绝新数据
     *
     * @param required 需要的字节数
     * @return true 表示可以容纳，false 表示无法容纳
     */
    private boolean tryEnsureCapacity(int required) {
        // 如果剩余空间足够，直接返回
        if (buffer.remaining() >= required) {
            return true;
        }

        // 尝试压缩缓冲区（移动已读数据到开头）
        buffer.compact();
        buffer.flip();
        buffer.compact();

        // 压缩后再次检查
        if (buffer.remaining() >= required) {
            return true;
        }

        // 如果新数据比整个缓冲区还大，无法处理
        if (required > buffer.capacity()) {
            logger.error("数据包过大，超过缓冲区容量: {} > {}", required, buffer.capacity());
            return false;
        }

        // 缓冲区已满，但新数据可以放入空缓冲区
        // 这里选择丢弃当前缓冲区中的半包数据（记录警告）
        int lostBytes = buffer.position();
        if (lostBytes > 0) {
            logger.warn("缓冲区已满，丢弃 {} bytes 未处理的半包数据", lostBytes);
        }
        buffer.clear();

        return buffer.remaining() >= required;
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

    // ==================== NMEA 处理 ====================

    private void notifyNmea(String nmea) {
        // 存储原始 NMEA 数据
        if (nmeaStorageService != null) {
            NmeaRecord record = new NmeaRecord(new Date(), nmea);
            nmeaStorageService.saveNmeaData(record);
        }

        // 处理 GGA 语句（提取历元时间）
        if (isGgaSentence(nmea)) {
            processGgaData(nmea);
        }

        // 处理 GSV 语句 - 直接委托给 fusionService
        if (fusionService != null && gsvParser != null && gsvParser.isGsvSentence(nmea)) {
            fusionService.processGsvData(nmea);
        }

        // 通知监听器
        if (listeners != null) {
            for (GnssDataListener listener : listeners) {
                try { listener.onNmeaReceived(nmea); } catch (Exception ignored) {}
            }
        }
    }

    /**
     * 判断是否为 GGA 语句
     */
    private boolean isGgaSentence(String nmea) {
        return nmea.startsWith(PREFIX_GNGGA) ||
                nmea.startsWith(PREFIX_GPGGA) ||
                nmea.startsWith(PREFIX_BDGGA);
    }

    /**
     * 处理 GGA 数据（提取历元时间和定位信息）
     */
    private void processGgaData(String ggaLine) {
        try {
            String[] parts = ggaLine.split(",", -1);
            if (parts.length < 10 || parts[2].isEmpty() || parts[4].isEmpty()) return;

            // 提取 UTC 时间作为历元时间
            if (parts.length > 1 && !parts[1].isEmpty()) {
                currentEpochTime = parseUtcTime(parts[1]);
                if (fusionService != null) {
                    fusionService.setEpochTime(currentEpochTime);
                }
            }

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

    /**
     * 解析 UTC 时间字符串为毫秒时间戳
     */
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
        } catch (Exception e) { return 0.0; }
    }

    // ==================== RTCM 处理 ====================

    private void notifyRtcm(byte[] data) {
        // 存储原始 RTCM 数据
        if (rtcmStorageService != null) {
            rtcmStorageService.saveRtcmRawData(data);
        }

        // 解算 RTCM 数据
        decodeRtcmData(data);

        // 通知监听器
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
                updateSatCache(obsArray, count, rtcmData);
            }
        } catch (UnsatisfiedLinkError e) {
            logger.error("找不到 rtklib_bridge.dll，请检查系统路径！");
        } catch (Exception e) {
            logger.error("RTCM 解码异常: {}", e.getMessage());
        }
    }

    /**
     * 更新卫星缓存
     */
    private void updateSatCache(RtklibNative.JavaObs[] newObs, int count, byte[] rtcmData) {
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
                fusionService.processRtcmData(satNo, rtcmMessageType, p1, p2, l1, l2, snr, c1, c2);
            }
        }

        // 定时触发融合入库
        long now = System.currentTimeMillis();
        long lastTime = lastPrintTime.get();
        if (now - lastTime > printIntervalMs) {
            if (lastPrintTime.compareAndSet(lastTime, now)) {
                fuseAndStoreData();
            }
        }
    }

    /**
     * 获取 RTCM 消息类型
     */
    private int getRtcmMessageType(byte[] rtcmData) {
        if (rtcmData == null || rtcmData.length < 6) {
            return 0;
        }
        return ((rtcmData[3] & 0xFF) << 4) | ((rtcmData[4] & 0xF0) >> 4);
    }

    /**
     * 执行数据融合和入库
     */
    private void fuseAndStoreData() {
        if (fusionService != null) {
            fusionService.fuseAndStore();
        }
    }

    // ==================== 公共接口 ====================

    public Long getCurrentEpochTime() {
        return currentEpochTime;
    }

    /**
     * 获取缓冲区溢出计数
     */
    public long getBufferOverflowCount() {
        return bufferOverflowCount.get();
    }
}
